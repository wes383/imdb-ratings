const https = require('https');
const fs = require('fs');
const zlib = require('zlib');
const csv = require('csv-parser');
const { neon } = require('@neondatabase/serverless');
const ws = require('ws');

const IMDB_RATINGS_URL = 'https://datasets.imdbws.com/title.ratings.tsv.gz';
const TEMP_FILE = '/tmp/title.ratings.tsv.gz';
const BATCH_SIZE = 500;

if (typeof WebSocket === 'undefined') {
  global.WebSocket = ws;
}

const sql = neon(process.env.DATABASE_URL || process.env.POSTGRES_URL, {
  fetchOptions: {
    timeout: 30000
  }
});

async function downloadFile(url, dest) {
  return new Promise((resolve, reject) => {
    const file = fs.createWriteStream(dest);
    https.get(url, (response) => {
      response.pipe(file);
      file.on('finish', () => {
        file.close();
        resolve();
      });
    }).on('error', (err) => {
      fs.unlink(dest, () => {});
      reject(err);
    });
  });
}

async function createTable() {
  await sql`
    CREATE TABLE IF NOT EXISTS imdb_ratings (
      imdb_id VARCHAR(10) PRIMARY KEY,
      rating DECIMAL(3,1) NOT NULL,
      num_votes INTEGER NOT NULL,
      updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
  `;
  
  await sql`CREATE INDEX IF NOT EXISTS idx_rating ON imdb_ratings(rating)`;
}

async function processData() {
  console.log('Downloading IMDb ratings dataset...');
  await downloadFile(IMDB_RATINGS_URL, TEMP_FILE);
  
  console.log('Creating table...');
  await createTable();
  
  console.log('Processing data...');
  let batch = [];
  let count = 0;
  const stream = fs.createReadStream(TEMP_FILE)
    .pipe(zlib.createGunzip())
    .pipe(csv({ separator: '\t' }));

  return new Promise((resolve, reject) => {
    stream.on('data', (row) => {
      if (row.tconst && row.averageRating && row.numVotes) {
        batch.push({
          imdbId: row.tconst,
          rating: parseFloat(row.averageRating),
          numVotes: parseInt(row.numVotes)
        });

        if (batch.length >= BATCH_SIZE) {
          stream.pause();
          const currentBatch = [...batch];
          batch = [];
          
          insertBatch(currentBatch)
            .then(() => {
              count += currentBatch.length;
              console.log(`Processed ${count} records...`);
              stream.resume();
            })
            .catch((error) => {
              console.error('Error inserting batch:', error);
              stream.resume();
            });
        }
      }
    })
    .on('end', async () => {
      if (batch.length > 0) {
        await insertBatch(batch);
        count += batch.length;
      }
      console.log(`Completed! Total records: ${count}`);
      fs.unlinkSync(TEMP_FILE);
      resolve();
    })
    .on('error', reject);
  });
}

async function insertBatch(batch) {
  if (batch.length === 0) return;
  
  const values = batch.map(item => 
    `('${item.imdbId.replace(/'/g, "''")}', ${item.rating}, ${item.numVotes}, CURRENT_TIMESTAMP)`
  ).join(',');

  const query = `
    INSERT INTO imdb_ratings (imdb_id, rating, num_votes, updated_at)
    VALUES ${values}
    ON CONFLICT (imdb_id) 
    DO UPDATE SET 
      rating = EXCLUDED.rating,
      num_votes = EXCLUDED.num_votes,
      updated_at = CURRENT_TIMESTAMP
  `;

  await sql(query);
}

processData()
  .then(() => {
    console.log('Data update completed successfully');
    process.exit(0);
  })
  .catch((error) => {
    console.error('Error updating data:', error);
    process.exit(1);
  });
