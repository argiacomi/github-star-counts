require('dotenv').config();
const { createClient } = require('@supabase/supabase-js');

const { SUPABASE_URL, SUPABASE_ANON_KEY, GITHUB_TOKEN } = process.env;

const MAX_RESULTS_PER_SEARCH = 1000;
const PER_PAGE = 100;
const INITIAL_STAR_RANGE = 1000;
const STAR_RANGE_DECREMENT = 100;
const STAR_MAX = 6000;

const supabase = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

const fetchSettings = {
  method: 'GET',
  headers: {
    Accept: 'application/vnd.github+json',
    Authorization: `Bearer ${GITHUB_TOKEN}`,
  },
};

async function fetchRateLimit() {
  const baseUrl = 'https://api.github.com';
  const rateLimitUrl = `${baseUrl}/rate_limit`;

  const rateLimit = await fetch(rateLimitUrl, fetchSettings);
  const rateLimitJson = await rateLimit.json();

  return rateLimitJson.resources.search;
}

function filterResponseData(responseJson) {
  const filteredRepos = responseJson.items.filter(
    ({ archived, disabled, visibility }) => !archived && !disabled && visibility === 'public',
  );

  return filteredRepos.map(
    ({
      id,
      full_name,
      html_url,
      description,
      language,
      forks_count,
      stargazers_count,
      watchers_count,
      open_issues_count,
      topics,
      contributors_url,
    }) => ({
      github_id: id,
      full_name,
      html_url,
      description,
      language,
      forks: forks_count,
      stars: stargazers_count,
      watchers: watchers_count,
      open_issues: open_issues_count,
      topics,
      contributors_url,
    }),
  );
}

async function fetchRepositoriesWithPagination(url, settings) {
  try {
    let nextPageUrl = url;
    const repoData = [];

    while (nextPageUrl) {
      const response = await fetch(nextPageUrl, settings);
      const data = await response.json();

      repoData.push(...filterResponseData(data));

      const linkHeader = response.headers.get('Link');
      nextPageUrl = null;

      if (linkHeader) {
        const links = linkHeader.split(',');

        for (const link of links) {
          const url = new URL(link.trim().split(';')[0].slice(1, -1));
          const rel = link.trim().split(';')[1].trim();

          if (rel === 'rel="next"') {
            nextPageUrl = url.href;
            break;
          }
        }
      }
    }

    return repoData;
  } catch (error) {
    console.error('Error paginating requests:', err.message);
    process.exitCode = 1;
  }
}

async function fetchAllRepos(starCountStart = 5000) {
  const baseUrl = 'https://api.github.com';
  const searchUrl = `${baseUrl}/search/repositories`;
  const repoData = [];

  let starCutoff = starCountStart;

  try {
    const { limit, remaining, reset } = await fetchRateLimit();
    console.info(`API rate limit: ${remaining} remaining of ${limit} until ${new Date(reset * 1000)}`);
    while (true) {
      let starCountRange = INITIAL_STAR_RANGE;
      let starCountParsedRange = `${starCutoff}..${Math.min(STAR_MAX, starCutoff + starCountRange - 1)}`;
      let data = {};

      console.info('1: Searching for optimal Star Range');

      while (data.total_count > MAX_RESULTS_PER_SEARCH || data.total_count === undefined) {
        console.info(`2: Trying Range ${starCountParsedRange}`);
        const queryParams = new URLSearchParams({
          q: `stars:${starCountParsedRange}`,
          per_page: PER_PAGE.toString(),
        });

        const response = await fetch(`${searchUrl}?${queryParams.toString()}`, fetchSettings);

        data = await response.json();

        if (data.total_count <= MAX_RESULTS_PER_SEARCH) {
          break;
        }

        starCountRange -= STAR_RANGE_DECREMENT;
        starCountParsedRange = `${starCutoff}..${Math.min(STAR_MAX, starCutoff + starCountRange - 1)}`;
      }

      console.info(`3: Optimal Star Range set at ${starCountParsedRange}`);

      const queryParams = new URLSearchParams({
        q: `stars:${starCountParsedRange}`,
        per_page: PER_PAGE.toString(),
      });

      const nextPageUrl = `${searchUrl}?${queryParams.toString()}`;
      const fetchedRepos = await fetchRepositoriesWithPagination(nextPageUrl, fetchSettings);

      repoData.push(...fetchedRepos);
      console.info(`4: Adding ${fetchedRepos.length} repositories. Total stored: ${repoData.length}`);

      starCutoff += starCountRange;

      if (starCutoff > STAR_MAX) {
        break;
      }
    }

    return repoData;
  } catch (error) {
    console.error('Error fetching data:', err.message);
    process.exitCode = 1;
  }
}

const main = async () => {
  try {
    const repoData = await fetchAllRepos();

    try {
      const response = await supabase
        .from('repositories')
        .upsert(repoData, { ignoreDuplicates: false, onConflict: 'github_id' })
        .select();

      if (response.data.length !== null) {
        console.info(`Successfully upserted ${response.data.length} repositories`);
      } else {
        console.info(response);
      }
    } catch (err) {
      console.error('Error upserting data:', err.message);
      process.exitCode = 1;
    }
  } catch (err) {
    console.error('Error fetching or upserting data:', err.message);
    process.exitCode = 1;
  }
};

main();
