import { BskyAgent } from "@atproto/api";

const agent = new BskyAgent({
  service: "https://bsky.social"
});

let loggedIn = false;

async function login() {
  if (loggedIn) return;

  await agent.login({
    identifier: process.env.BLUESKY_HANDLE,
    password: process.env.BLUESKY_APP_PASSWORD
  });

  loggedIn = true;

  console.log("🦋 Bluesky authenticated");
}

export async function postToBluesky(text) {
  await login();

  if (!text || !text.trim()) {
    throw new Error("Bluesky post cannot be empty");
  }

  const trimmedText = text.trim();

  if ([...trimmedText].length > 300) {
    throw new Error(
      `Bluesky post exceeds 300 characters (${[...trimmedText].length})`
    );
  }

  const result = await agent.post({
    text: trimmedText
  });

  console.log("🦋 Bluesky post published");

  return result;
}
