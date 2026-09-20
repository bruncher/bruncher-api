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

async function post(text) {
  await login();

  if (!text || !text.trim()) {
    throw new Error('Bluesky post cannot be empty');
  }

  if ([...text].length > 300) {
    throw new Error('Bluesky post exceeds 300 characters');
  }

  return await agent.post({
    text: text.trim()
  });
}

module.exports = {
  post
};
