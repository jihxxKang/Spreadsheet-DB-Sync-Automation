//'use strict';

const { google } = require("googleapis");
const { Pool } = require("pg");

const auth = new google.auth.GoogleAuth({
  keyFile: "./hoooking-812486196d32.json",
  scopes: ["https://www.googleapis.com/auth/spreadsheets"],
});

const redshiftConfig = {
  user: "admin",
  host: "redshift-cluster-1.cwt6gkhqiwcs.ap-northeast-2.redshift.amazonaws.com",
  database: "dev",
  password: "Hamzzi0426!",
  port: 5439,
};

const spreadsheetId = "17h2d1FHhFV3DYXRAAHzk0H7VwIoE_obKoDoi9s1B3B8";

const onlineSheetName = "Online 비용";
const onlineTableName = "online_marketing";

const offlineSheetName = "Offline 비용";
const offlineTableName = "offline_marketing";

async function fetchOnlineMarketingDataFromSpreadsheet() {
  try {
    // Authorize and create the Sheets client
    const authClient = await auth.getClient();
    const sheets = google.sheets({ version: "v4", auth: authClient });
    // Compute the range string based on the specified columns and start row
    const range = `${onlineSheetName}!A:Q`;
    // Fetch the data from the spreadsheet
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range,
    });

    const values = response.data.values;
    if (!values || values.length === 0) {
      console.log("No data found in the specified range.");
      return;
    }

    // Filter
    const wonToInt = (won) => {
      return parseInt(won.split(",").join("").slice(1));
    };

    const nonEmptyRows = values.slice(2).filter((row) => row[10].length > 1);
    const validRows = nonEmptyRows.filter((row) => row[16][0] === "₩");
    const toInsert = validRows.map((x) => ({
      os: x[3],
      medium: x[4],
      sigungu: x[6],
      date: x[10]
        .split(". ")
        .map((x) => (x.length === 1 ? "0" + x : x))
        .join("-"),
      campaign_name: x[11],
      ad_name: x[12],
      cost: wonToInt(x[16]),
    }));

    // Connect to Redshift
    const pool = new Pool(redshiftConfig);
    const client = await pool.connect();

    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS ${onlineTableName} (
        os VARCHAR,
        medium VARCHAR,
        sigungu VARCHAR,
        date VARCHAR,
        campaign_name VARCHAR,
        ad_name VARCHAR,
        cost INT
      );
    `;
    await client.query(createTableQuery);

    const truncateTableQuery = `
      TRUNCATE ${onlineTableName};
    `;
    await client.query(truncateTableQuery);

    const insertQuery = `
      INSERT INTO ${onlineTableName} (os, medium, sigungu, date, campaign_name, ad_name, cost)
      VALUES ${toInsert
        .map(
          (row) =>
            `('${row.os}', '${row.medium}', '${row.sigungu}', '${row.date}', '${row.campaign_name}', '${row.ad_name}', ${row.cost})`
        )
        .join(", ")};
    `;
    await client.query(insertQuery);
  } catch (error) {
    console.error("Error during online marketing data sync:", error);
  }
}

async function fetchOfflineMarketingDataFromSpreadsheet() {
  try {
    // Authorize and create the Sheets client
    const authClient = await auth.getClient();
    const sheets = google.sheets({ version: "v4", auth: authClient });
    // Compute the range string based on the specified columns and start row
    const range = `${offlineSheetName}!A:M`;
    // Fetch the data from the spreadsheet
    const response = await sheets.spreadsheets.values.get({
      spreadsheetId,
      range,
    });
    const values = response.data.values;
    if (!values || values.length === 0) {
      console.log("No data found in the specified range.");
      return;
    }

    // Filter
    const wonToInt = (won) => {
      return parseInt(won.split(",").join("").slice(1));
    };
    const toDateObject = (excelDateString) => {
      const str =
        excelDateString
          .split(". ")
          .map((x) => (x.length === 1 ? "0" + x : x))
          .join("-") + "T12:00:00+09:00";
      return new Date(str);
    };
    const dateDiff = (excelDateStart, excelDateEnd) => {
      return (
        (toDateObject(excelDateEnd) - toDateObject(excelDateStart)) /
        24 /
        3600 /
        1000
      );
    };

    const nonEmptyRows = values.filter((row) => row[0].length > 1).slice(2);
    const validRows = nonEmptyRows.filter(
      (row) => row[8][0] === "₩" && row[11] && row[12]
    );
    const formattedRows = validRows.map((x) => [
      x[0], //sigungu
      x[3], //category
      x[4], //detail
      wonToInt(x[8]), //cost
      x[11], //start_date
      x[12], //end_date
      1 + dateDiff(x[11], x[12]), //duration
    ]);
    const toInsert = [];
    formattedRows.forEach((row) => {
      const dailyCost = Math.round(row[3] / row[6]);
      const cur = toDateObject(row[4]);
      for (let i = 0; i < row[6]; i++) {
        toInsert.push({
          sigungu: row[0],
          category: row[1],
          detail: row[2],
          cost: dailyCost,
          date: cur
            .toLocaleString("ko-KR") //"2012. 12. 20. 오전 3:00:00"
            .split(". ")
            .slice(0, 3)
            .map((x) => (x.length === 1 ? "0" + x : x))
            .join("-"),
        });
        cur.setDate(cur.getDate() + 1);
      }
    });

    // Connect to Redshift
    const pool = new Pool(redshiftConfig);
    const client = await pool.connect();

    const createTableQuery = `
      CREATE TABLE IF NOT EXISTS ${offlineTableName} (
        sigungu VARCHAR,
        category VARCHAR,
        detail VARCHAR,
        cost INT,
        date VARCHAR
      );
    `;
    await client.query(createTableQuery);

    const truncateTableQuery = `
      TRUNCATE ${offlineTableName};
    `;
    await client.query(truncateTableQuery);

    const insertQuery = `
      INSERT INTO ${offlineTableName} (sigungu, category, detail, cost, date)
      VALUES ${toInsert
        .map(
          (row) =>
            `('${row.sigungu}', '${row.category}', '${row.detail}', ${row.cost}, '${row.date}')`
        )
        .join(", ")};
    `;
    await client.query(insertQuery);
  } catch (error) {
    console.error("Error during offline marketing data sync:", error);
  }
}

async function handler() {
  await fetchOnlineMarketingDataFromSpreadsheet();
  await fetchOfflineMarketingDataFromSpreadsheet();
}

module.exports = { handler };
