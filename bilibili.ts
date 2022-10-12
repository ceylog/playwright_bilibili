const { chromium } = require("playwright");
const { Client } = require("@elastic/elasticsearch");
const fs = require("fs");

let index_name = "bilibili";

const esClient = new Client({
  node: "http://localhost:9200",
  // node: "https://localhost:9200",
  // auth: {
  //   username: "elastic",
  //   password: "*fy_BBoR5HI*1uoeiZok",
  // },
  // tls: {
  //   ca: fs.readFileSync(
  //     "/Users/sam/Downloads/elasticsearch-8.4.2/config/certs/http_ca.crt"
  //   ),
  //   rejectUnauthorized: false,
  // },
});

const saveDoc = async (data) => {
  const operations = data.flatMap((doc) => [
    { index: { _index: index_name } },
    doc,
  ]);
  await esClient.bulk({ operations });
};

const search = async (keyword) => {
  const browser = await chromium.launch({ headless: true });
  const context = await browser.newContext();

  const page = await context.newPage();

  await page.goto("https://search.bilibili.com/video");

  // Click [placeholder="输入关键字搜索"]
  await page.locator('[placeholder="输入关键字搜索"]').click();
  // Fill [placeholder="输入关键字搜索"]
  await page.locator('[placeholder="输入关键字搜索"]').fill(keyword);
  let xhrCatcher = page.waitForResponse(
    (r) =>
      r.request().url().includes("/x/web-interface/search/type") &&
      r.request().method() != "OPTIONS" &&
      r.status() === 200
  );
  // Press Enter
  await page.locator('[placeholder="输入关键字搜索"]').press("Enter");

  let xhrResponse = await xhrCatcher;
  let xhrPayload = await xhrResponse.json();
  console.log(
    "xhrPayload page %d size %d ",
    xhrPayload.data.page,
    xhrPayload.data.result.length
  );
  let data = xhrPayload.data.result;

  index_name = `bilibili_${keyword}`;

  const existsIdx = await esClient.indices.exists({ index: index_name });
  if (existsIdx) {
    //clean old data
    const delResult = await esClient.deleteByQuery({
      index: index_name,
      query: { match: { type: "video" } },
    });
    console.log(
      "old data is deleted, total %d deleted %d",
      delResult.total,
      delResult.deleted
    );
  } else {
    await esClient.indices.create({
      index: index_name,
      settings: {
        index: { number_of_replicas: 1, number_of_shards: 1 },
        analysis: { analyzer: { comma: { type: "pattern", pattern: "," } } },
      },
      mappings: {
        properties: {
          aid: {
            type: "long",
          },
          arcrank: {
            type: "text",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          arcurl: {
            type: "text",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          author: {
            type: "text",
            analyzer: "ik_smart",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          badgepay: {
            type: "boolean",
          },
          bvid: {
            type: "text",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          corner: {
            type: "text",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          cover: {
            type: "text",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          danmaku: {
            type: "long",
          },
          desc: {
            type: "text",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          description: {
            type: "text",
            analyzer: "ik_smart",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          duration: {
            type: "text",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          favorites: {
            type: "long",
          },
          hit_columns: {
            type: "text",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          id: {
            type: "long",
          },
          is_pay: {
            type: "long",
          },
          is_union_video: {
            type: "long",
          },
          like: {
            type: "long",
          },
          mid: {
            type: "long",
          },
          pic: {
            type: "text",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          play: {
            type: "long",
          },
          pubdate: {
            type: "long",
          },
          rank_score: {
            type: "long",
          },
          rec_reason: {
            type: "text",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          review: {
            type: "long",
          },
          senddate: {
            type: "long",
          },
          tag: {
            type: "text",
            analyzer: "comma",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          title: {
            type: "text",
            analyzer: "ik_smart",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          type: {
            type: "text",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          typeid: {
            type: "text",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          typename: {
            type: "text",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          upic: {
            type: "text",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          url: {
            type: "text",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
          video_review: {
            type: "long",
          },
          view_type: {
            type: "text",
            fields: {
              keyword: {
                type: "keyword",
                ignore_above: 256,
              },
            },
          },
        },
      },
    });
    console.log(`index ${index_name} created`);
  }

  await saveDoc(data);
  let hasNext = await page.locator('button:text("下一页")').isEnabled();
  console.log("hasNextPage ", hasNext);
  while (hasNext) {
    xhrCatcher = page.waitForResponse(
      (r) =>
        r.request().url().includes("/x/web-interface/search/type") &&
        r.request().method() != "OPTIONS" &&
        r.status() === 200
    );
    // Click text=下一页
    await page.locator("text=下一页").click();
    xhrResponse = await xhrCatcher;
    xhrPayload = await xhrResponse.json();
    await saveDoc(xhrPayload.data.result);
    console.log(
      "xhrPayload page %d size %d",
      xhrPayload.data.page,
      xhrPayload.data.result.length
    );
    hasNext = await page.locator('button:text("下一页")').isEnabled();
    console.log("hasNextPage ", hasNext);
  }

  await context.close();
  await browser.close();
  await esClient.indices.refresh({ index: index_name });
  const count = await esClient.count({ index: index_name });
  console.log("all done, total count ", count.count);
};

search("netty");
