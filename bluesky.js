const { BskyAgent } = require('@atproto/api');

const agent = new BskyAgent({
  service: 'https://bsky.social'
});

let loggedIn = false;

async function login() {
  if (loggedIn) return;

  await agent.login({
    identifier: process.env.BLUESKY_HANDLE,
    password: process.env.BLUESKY_APP_PASSWORD
  });

  loggedIn = true;
}

async function postToBluesky(text) {
  await login();

  if (!text || !text.trim()) {
    throw new Error('Bluesky post cannot be empty.');
  }

  const post = await agent.post({
    text: text.trim()
  });

  return {
    uri: post.uri,
    cid: post.cid
  };
}

module.exports = {
  postToBluesky
};
