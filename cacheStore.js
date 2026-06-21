import axios from "axios";

const OWNER = process.env.GH_OWNER;
const REPO = process.env.GH_REPO;
const TOKEN = process.env.GH_TOKEN;

if (!OWNER || !REPO || !TOKEN) {
  throw new Error("Missing GH_OWNER, GH_REPO, or GH_TOKEN env vars");
}

function headers() {
  return {
    Authorization: `Bearer ${TOKEN}`,
    Accept: "application/vnd.github+json",
    "X-GitHub-Api-Version": "2022-11-28"
  };
}

export async function loadCache(filePath) {
  try {
    const res = await axios.get(
      `https://api.github.com/repos/${OWNER}/${REPO}/contents/${filePath}`,
      { headers: headers() }
    );

    const json = Buffer.from(res.data.content, "base64").toString("utf-8");
    return JSON.parse(json);
  } catch (err) {
    if (err.response?.status === 404) return null;
    throw err;
  }
}

export async function saveCache(filePath, data, message = "update cache") {
  const content = Buffer.from(
    JSON.stringify(data, null, 2)
  ).toString("base64");

  let sha;

  // GitHub requires SHA if file exists
  try {
    const existing = await axios.get(
      `https://api.github.com/repos/${OWNER}/${REPO}/contents/${filePath}`,
      { headers: headers() }
    );

    sha = existing.data.sha;
  } catch (err) {
    if (err.response?.status !== 404) throw err;
  }

  await axios.put(
    `https://api.github.com/repos/${OWNER}/${REPO}/contents/${filePath}`,
    {
      message,
      content,
      sha
    },
    { headers: headers() }
  );
}
