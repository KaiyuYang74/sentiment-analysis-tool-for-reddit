async function fetchSubreddits() {
  const resp = await fetch('/api/subreddits');
  const data = await resp.json();
  return data; // e.g. { subreddits: ['news','askreddit'] }
}

async function addSubreddit(name) {
  await fetch('/api/subreddits', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ subreddit: name })
  });
}

async function fetchResults() {
  const resp = await fetch('/api/results');
  return await resp.json(); // e.g. [ { post_id, subreddit, word_freq_json, sentiment_result, updated_time }, ... ]
}

document.addEventListener('DOMContentLoaded', async () => {
  const listEl = document.getElementById('subreddit-list');
  const inputEl = document.getElementById('subreddit-input');
  const addBtn = document.getElementById('add-btn');
  const resultsBtn = document.getElementById('load-results-btn');

  // 加载并显示当前 subreddits
  async function loadSubreddits() {
    const data = await fetchSubreddits();
    listEl.innerHTML = '';
    if (data && data.subreddits) {
      data.subreddits.forEach((sub) => {
        const li = document.createElement('li');
        li.textContent = sub;
        listEl.appendChild(li);
      });
    }
  }

  // 添加新的subreddit
  addBtn.addEventListener('click', async () => {
    const name = inputEl.value.trim();
    if (!name) return;
    await addSubreddit(name);
    inputEl.value = '';
    await loadSubreddits();
  });

  // 查看分析结果
  resultsBtn.addEventListener('click', async () => {
    const resultsContainer = document.getElementById('results-container');
    resultsContainer.innerHTML = '';

    const rows = await fetchResults();

    // group rows by subreddit
    const grouped = {};
    rows.forEach(row => {
      const subreddit = row.subreddit || 'unknown';
      if (!grouped[subreddit]) {
        grouped[subreddit] = [];
      }
      grouped[subreddit].push(row);
    });

    // function to parse freq JSON and keep top 5
    function formatTopFiveFreq(freqJson) {
      if (!freqJson) return '';
      try {
        const freqObj = JSON.parse(freqJson);
        const sorted = Object.entries(freqObj).sort((a, b) => b[1] - a[1]);
        const top5 = sorted.slice(0, 5);
        return top5.map(([w, f]) => `${w}(${f})`).join(', ');
      } catch (e) {
        return freqJson;
      }
    }

    // create a table for each subreddit
    Object.keys(grouped).forEach(sub => {
      const heading = document.createElement('h3');
      heading.textContent = `Subreddit: ${sub}`;
      resultsContainer.appendChild(heading);

      const table = document.createElement('table');
      table.border = '1';

      const thead = document.createElement('thead');
      thead.innerHTML = `\n    <tr>\n      <th>Post ID</th>\n      <th>Title</th>\n      <th>Top 5 Word Freq</th>\n      <th>Sentiment</th>\n      <th>Updated Time</th>\n    </tr>\n  `;
      table.appendChild(thead);

      const tbody = document.createElement('tbody');

      grouped[sub].forEach(row => {
        const tr = document.createElement('tr');
        const tdPostId = document.createElement('td');
        const tdTitle = document.createElement('td');
        const tdFreq = document.createElement('td');
        const tdSentiment = document.createElement('td');
        const tdUpdated = document.createElement('td');

        tdPostId.textContent = row.post_id;
        let displayedTitle = row.title || "";
        if (displayedTitle.length > 40) {
          displayedTitle = displayedTitle.slice(0, 40) + "...";
        }
        tdTitle.textContent = displayedTitle;
        tdFreq.textContent = formatTopFiveFreq(row.word_freq_json);
        tdSentiment.textContent = row.sentiment_result;
        tdUpdated.textContent = row.updated_time;

        tr.appendChild(tdPostId);
        tr.appendChild(tdTitle);
        tr.appendChild(tdFreq);
        tr.appendChild(tdSentiment);
        tr.appendChild(tdUpdated);
        tbody.appendChild(tr);
      });

      table.appendChild(tbody);
      resultsContainer.appendChild(table);
    });
  });

  // 页面初始化时加载
  await loadSubreddits();
});