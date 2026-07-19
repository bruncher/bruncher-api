import express from "express";
import axios from "axios";
import cors from "cors";
import { loadCache, saveCache } from "./cacheStore.js";

export async function mountGaming(app) {
  const router = express.Router();
  const allowedOrigins = ["https://bruncher.github.io", "http://localhost:3000"];
  
  router.use(cors({
    origin: function(origin, callback) {
      if (!origin || allowedOrigins.includes(origin)) {
        callback(null, true);
      } else {
        console.warn("Blocked CORS request from:", origin);
        callback(new Error("Not allowed by CORS"));
      }
    }
  }));
  
  // In-memory cache per currency
  let cache = {};
  const CACHE_TTL = 1000 * 60 * 60; // 1 hour
  const CURRENCIES = ["USD"]; // CheapShark deals are only available in USD
  const DEFAULT_STORES = ["steam", "humble store", "fanatical"].map(s => s.toLowerCase().trim());
  
  const STORE_PRIORITY = {
    "steam": 1,
    "humble store": 2,
    "fanatical": 3
  };
  let storeMap = {};
  let defaultStoreIDs = [];
  
  /**
   * Load storeID -> storeName map
   */
  async function loadStores() {
    try {
      const res = await axios.get("https://www.cheapshark.com/api/1.0/stores");
      storeMap = Object.fromEntries(
        res.data.map(s => [s.storeID, s.storeName.toLowerCase().trim()])
      );
      console.log("Store map loaded:", Object.keys(storeMap).length, "stores");
  
      defaultStoreIDs = Object.entries(storeMap)
        .filter(([id, name]) => DEFAULT_STORES.includes(name))
        .map(([id]) => id);
      
      console.log("Default store IDs:", defaultStoreIDs);
  
    } catch (err) {
      console.error("Error loading stores:", err.message);
    }
  }
  
  /**
   * Fetch deals from CheapShark (multiple pages) and store in cache
   */
  async function fetchDeals(currency, storeIDs) {
    if (!Array.isArray(storeIDs)) {
      console.error("fetchDeals called without a valid storeIDs array!");
      return;
    }
  
    if (storeIDs.length === 0) {
      console.error("fetchDeals aborted: no valid storeIDs found");
      return;
    }
    
    try {
      let page = 0;
      const uniqueGames = {};
      //const pagesFetched = []; // not being used now
  
      // Keep fetching until we have target number unique games or reach max pages
      const TARGET_UNIQUE_DEALS = 2000;
      const MAX_DEAL_PAGES = 100;
      
      while (
        Object.keys(uniqueGames).length < TARGET_UNIQUE_DEALS &&
        page < MAX_DEAL_PAGES
      ) {
        let response = null;
        let retryAfter = null;
        let attempts = 0;
        const MAX_ATTEMPTS = 10;

        while (attempts < MAX_ATTEMPTS) {
          try {        
            response = await axios.get("https://www.cheapshark.com/api/1.0/deals", {
              params: {
                pageSize: 100,
                pageNumber: page,
                cc: currency,
                storeID: storeIDs.join(",")
              }
            });

            break; // success → exit loop
          } catch (err) {
            attempts++;

            const status = err.response?.status;
            retryAfter = err.response?.headers?.["retry-after"]
              ? parseInt(err.response.headers["retry-after"], 10)
              : null;
        
            // backoff delay
            const delay = 1000 * attempts;
        
            console.warn(
              `CheapShark error (${status || "no-status"}) for ${currency} page ${page} → retry ${attempts}/${MAX_ATTEMPTS} in ${delay}ms` +
              (retryAfter ? ` | Retry-After: ${retryAfter}s` : "")
            );
        
            if (attempts >= MAX_ATTEMPTS) {
              console.error(`Max attempts reached on page ${page} of ${currency}`);
              break;
            }
        
            await new Promise(r => setTimeout(r, delay));
          }
        }
        
        if (!response) {
          console.warn(`No response after max retries for ${currency} page ${page}`);

          const cooldown = retryAfter ?? 30;
                  
          console.warn(`⚠️ Cooling down ${cooldown}s before retrying...`);
          await new Promise(r => setTimeout(r, cooldown * 1000));
          
          continue;
        }
              
        let newDealsThisPage = 0;
      
        for (const deal of response.data) {
          deal.storeName = storeMap[deal.storeID] || "unknown";
      
          // Skip deals without Steam App ID
          if (!deal.steamAppID) continue;
      
          const gameId = deal.gameID;
          const current = uniqueGames[gameId];
          const dealStore = deal.storeName.toLowerCase();
          const dealPrice = parseFloat(deal.salePrice);
      
          if (!STORE_PRIORITY[dealStore]) continue;
      
          if (!current) {
            uniqueGames[gameId] = deal;
            newDealsThisPage++;
            continue;
          }
      
          const currentStore = current.storeName.toLowerCase();
          const currentPrice = parseFloat(current.salePrice);
      
          if (dealPrice < currentPrice) {
            uniqueGames[gameId] = deal;
            continue;
          }
      
          if (dealPrice === currentPrice) {
            const dealPrio = STORE_PRIORITY[dealStore];
            const currentPrio = STORE_PRIORITY[currentStore];
            if (dealPrio < currentPrio) uniqueGames[gameId] = deal;
          }
        }
      
        //pagesFetched.push({ page: page + 1, dealsFetched: newDealsThisPage });  // log after each page instead

        console.log(
          `Page ${page + 1}: +${newDealsThisPage} unique (${Object.keys(uniqueGames).length}/${TARGET_UNIQUE_DEALS})`
        );

        await new Promise(r => setTimeout(r, 250));
      
        page++;
      }
  
      // Convert collected unique games into an array
      const uniqueDeals = Object.values(uniqueGames);
  
      // Reattach previously cached Steam metadata
      for (const deal of uniqueDeals) {
        const id = String(deal.steamAppID);
        if (steamMetaCache[id] !== undefined) {
          deal.steamMeta = steamMetaCache[id];
        } else {
          deal.steamMeta = null; // placeholder until enrichment fills it
        }
      }
  
      const newBlock = {
        timestamp: Date.now(),
        data: uniqueDeals
      };
      
      // Atomic swap — ensures no partial metadata window
      cache[currency] = newBlock;

      try {
        await saveCache(
          "gamingDealsCache.json",
          cache,
          `Updated gaming deals cache (${uniqueDeals.length} deals)`
        );
      
        console.log(
          `💾 Gaming deals saved: ${uniqueDeals.length} deals (${getObjectSizeKB(cache)} KB)`
        );
      
      } catch (err) {
        console.error("❌ Failed to save gaming deals:", err.message);
      }
  
      //console.log(`Pages fetched:`, pagesFetched); // doing log per page found instead

      console.log(`Cache updated for ${currency} with ${uniqueDeals.length} unique deals`);
  
    } catch (err) {
      console.error(`Error fetching deals for ${currency}:`, err.message);
    }
  }
  
  /**
   * Pre-warm USD on startup
   */
  async function preWarm() {
    await loadStores();

    const lastUpdate = cache.USD?.timestamp || 0;
    const age = Date.now() - lastUpdate;
    
    console.log(
      `Gaming deals cache age: ${(age / 1000 / 60).toFixed(1)} minutes`
    );

    if (cache.USD && age < CACHE_TTL) {
      console.log("Gaming cache still fresh. Skipping refresh.");
      return;
    }
     
    for (const currency of CURRENCIES) {
      await fetchDeals(currency, defaultStoreIDs);
    }
  }
  
  /**
   * Set up hourly update for all currencies (USD only), and enrich with steam data
   */
  setInterval(async () => {
    for (const currency of CURRENCIES) {
      await fetchDeals(currency, defaultStoreIDs);
    }
  
    await enrichWithSteamData(cache["USD"]?.data || [])
      .catch(err => console.error("Hourly Steam enrichment failed:", err));

    console.log(
      `⏰ Next gaming refresh: ${new Date(Date.now() + CACHE_TTL).toLocaleString("en-CA", {
        timeZone: "America/Toronto"
      })}`
    );
    
  }, CACHE_TTL);

  
  // Global cache for Steam metadata
  const steamMetaCache = {};

  function logSteamMetaSummary(prefix = "") {
    const games = Object.values(steamMetaCache);
  
    const genreCounts = {};
  
    for (const game of games) {
      if (!game?.genres) continue;
  
      for (const genre of game.genres) {
        genreCounts[genre] = (genreCounts[genre] || 0) + 1;
      }
    }
  
    console.log(`🎮 ${prefix} Genre Summary`);
  
    if (Object.keys(genreCounts).length > 0) {
      console.table(
        Object.entries(genreCounts)
          .sort((a, b) => b[1] - a[1])
          .map(([genre, count]) => ({
            Genre: genre,
            Games: count
          }))
      );
    }
  }

  function getObjectSizeKB(obj) {
    const bytes = Buffer.byteLength(JSON.stringify(obj), "utf8");
    return (bytes / 1024).toFixed(1);
  }
  
  /**
   * GET /deals
   * Optional query: ?currency=USD
   */
  router.get("/deals", async (req, res) => {
    try {
      const currency = (req.query.currency || "USD").toUpperCase();
  
      const entry = cache[currency];
      const isExpired = !entry || (Date.now() - entry.timestamp > CACHE_TTL);
      const wasCached = !!entry && !isExpired;
  
      if (isExpired) {
        fetchDeals(currency, defaultStoreIDs).catch(console.error);
      }
  
      res.json({
        success: true,
        cached: wasCached,
        currency,
        count: entry ? entry.data.length : 0,
        deals: entry ? entry.data : []
      });
    } catch (err) {
      console.error(err);
      res.status(500).json({ success: false, error: err.message });
    }
  });
  
  router.get("/", (req, res) => {
    res.json({ status: "ok", message: "gaming-api live" });
  });
  
  router.get("/debug/cache", (req, res) => {
    res.json({
      usd: cache.USD?.data.length || 0,
      steamMetaCount: Object.keys(steamMetaCache).length
    });
  });
  
  router.get("/ping", (req, res) => {
    res.json({ status: "alive", time: Date.now() });
  });

  // --- mount router ---
  app.use("/gaming", router);
  
  // ==========================
  // Revised Steam enrichment
  // ==========================
  async function enrichWithSteamData(deals) {
    // Deduplicate by Steam ID
    const seen = new Set();
    const steamDeals = deals.filter(d => {
      if (!d.steamAppID) return false;
      const id = String(d.steamAppID);
      if (seen.has(id)) return false;
      seen.add(id);
      return steamMetaCache[id] === undefined || steamMetaCache[id] === null;
    });
  
    if (steamDeals.length === 0) {
      console.log("Steam enrichment: nothing to enrich, all metadata already cached.");
      return;
    }
    
    console.log(`Steam enrichment: ${steamDeals.length} new or missing metadata items.`);
  
    const sleep = ms => new Promise(r => setTimeout(r, ms));
    let enrichedCount = 0;
  
    for (const deal of steamDeals) {
      const id = String(deal.steamAppID);
  
      // Skip if cached with real data
      if (steamMetaCache[id] !== undefined && steamMetaCache[id] !== null) {
        deal.steamMeta = steamMetaCache[id];
        enrichedCount++;

        continue;
      }
  
      // Retry logic for 429 with soft max attempts
      let attempts = 0;
      const MAX_ATTEMPTS = 30;
      let done = false;
      
      while (!done && attempts < MAX_ATTEMPTS) {
        attempts++;
        try {
          const res = await axios.get(
            "https://store.steampowered.com/api/appdetails",
            { params: { appids: id, l: "en", cc: "us" }, timeout: 6000 }
          );
      
          const info = res.data[id];
          if (!info || !info.success) {
            deal.steamMeta = null;
          } else {
            const data = info.data;
            const date = data.release_date?.date;
            deal.steamMeta = {
              name: data.name,
              release_date: date || null,
              year: date && /\d{4}/.test(date) ? date.match(/\d{4}/)[0] : null,
              genres: data.genres?.map(g => g.description) || [],
              publishers: data.publishers || [],
              rating: data.metacritic?.score || null
            };
          }
      
          steamMetaCache[id] = deal.steamMeta;
  
          // Also update current deals inside USD cache
          for (const currency of CURRENCIES) {
            const cur = cache[currency];
            if (!cur || !cur.data) continue;
          
            const match = cur.data.find(d => String(d.steamAppID) === id);
            if (match) {
              match.steamMeta = deal.steamMeta;
            }
          }
          done = true;
      
          if (attempts > 1) {
            console.log(`✅ Steam app ${id} succeeded after ${attempts} attempts (backoff completed)`);
          }
      
        } catch (err) {
          const status = err.response?.status;
        
          if (status === 429 || status === 403) {
            // exponential backoff for both 429 and 403
            const delay = Math.min(30000, 1000 * Math.pow(2, attempts));
            console.warn(`${status} for ${id}, retrying in ${delay}ms (attempt ${attempts})`);
            await sleep(delay);
            // do not set done = true; retry will continue
          } else {
            console.error(`Steam meta error for ${id}:`, err.message);
            deal.steamMeta = null;
            steamMetaCache[id] = null;
            done = true; // stop retries for other errors
          }
        }
      }
      
      if (!done) {
        console.warn(`Max attempts reached for ${id}, skipping...`);
      }
  
      enrichedCount++;
      if (enrichedCount % 5 === 0 || enrichedCount === steamDeals.length) {
        console.log(`Enriched Steam metadata: ${enrichedCount}/${steamDeals.length}`);
      }
  
      await sleep(150); // throttle normal requests
    }
  
    console.log(`Finished enriching Steam data: ${enrichedCount}/${steamDeals.length} deals`);

    // backup the most recent Steam meta data called to GitHub in case of server restart to reload from there instead of re-polling data
    try {
      const steamCount = Object.keys(steamMetaCache).length;
      
      await saveCache(
        "steamMetaCache.json",
        steamMetaCache,
        `Updated Steam metadata (${steamCount} games)`
      );
      
      console.log(
        `💾 Steam metadata saved to GitHub: ${steamCount} games (${getObjectSizeKB(steamMetaCache)} KB)`
      );
      
      logSteamMetaSummary("Saved");
      
    } catch (err) {
      console.error("❌ Failed to save Steam metadata:", err.message);
    }
  }
  
  (async () => {
    console.log("Gaming: Starting server…");
  
    // Load Steam metadata cache from GitHub
    try {
      const saved = await loadCache("steamMetaCache.json");
  
      if (saved) {
        Object.assign(steamMetaCache, saved);

        console.log(
          `📥 Loaded Steam metadata cache: ${Object.keys(steamMetaCache).length} games (${getObjectSizeKB(steamMetaCache)} KB)`
        );
        logSteamMetaSummary("Loaded");
      } else {
        console.log("📥 No saved Steam metadata found.");
      }
    } catch (err) {
      console.error("Failed to load Steam metadata:", err.message);
    }

    // load deals from cache
    try {
      const savedDeals = await loadCache("gamingDealsCache.json");
    
      if (savedDeals) {
        cache = savedDeals;
    
        const usdCount = cache.USD?.data?.length || 0;
    
        console.log(
          `📥 Loaded gaming deals cache: ${usdCount} deals (${getObjectSizeKB(cache)} KB)`
        );
      } else {
        console.log("📥 No saved gaming deals found.");
      }
    
    } catch (err) {
      console.error("Failed to load gaming deals:", err.message);
    }
  
    console.log("Gaming: Loading stores and warming cache...");
  
    await preWarm(); // fetch CheapShark deals
    console.log("Warmup complete.");
    
    // Start Steam enrichment after cache warmup
    const allDeals = cache["USD"]?.data || [];
    
    if (allDeals.length > 0) {
      await enrichWithSteamData(allDeals)
        .catch(err => console.error("Initial Steam enrichment failed:", err));
    }
    
    console.log(
      `⏰ Next gaming refresh: ${new Date(Date.now() + CACHE_TTL).toLocaleString("en-CA", {
        timeZone: "America/Toronto"
      })}`
    );
    
    // Periodic status update -- hide for now because it seems redundant as the refreshes log things as well
    //setInterval(() => {
    //  const usdCount = cache.USD?.data.length || 0;
      
    //  console.log(
    //    `Status update: USD deals ${usdCount}, Steam cache ${Object.keys(steamMetaCache).length}`
    //  );
    //}, 60 * 60 * 1000); // every hour
  
  })();
  
  // ==========================
  // Run enrichment once per day
  // ==========================
  // blocking for now because we are enriching during the hourly deals check since the steam meta update should be fast now
  //setInterval(() => {
   // const combined = [
   //   ...(cache["USD"]?.data || []),
    //  ...(cache["CAD"]?.data || [])
    //];
  
    //if (combined.length > 0) {
      //enrichWithSteamData(combined).catch(console.error);
    //}
  //}, 24 * 60 * 60 * 1000); // daily

  console.log("🕹️ gaming API mounted");
}
