import { neon } from '@neondatabase/serverless';

const sql = neon(process.env.DATABASE_URL || process.env.POSTGRES_URL);

export default async function handler(req, res) {
  if (req.method !== 'GET') {
    return res.status(405).json({ error: 'Method not allowed' });
  }

  const { imdbId } = req.query;

  if (!imdbId || !imdbId.match(/^tt\d{7,8}$/)) {
    return res.status(400).json({ 
      error: 'Invalid IMDb ID format. Expected format: tt1234567' 
    });
  }

  try {
    const result = await sql`
      SELECT imdb_id, rating, num_votes, updated_at
      FROM imdb_ratings
      WHERE imdb_id = ${imdbId}
    `;

    if (result.length === 0) {
      return res.status(404).json({ 
        error: 'IMDb ID not found' 
      });
    }

    const data = result[0];
    
    return res.status(200).json({
      imdbId: data.imdb_id,
      rating: parseFloat(data.rating),
      numVotes: parseInt(data.num_votes),
      updatedAt: data.updated_at
    });

  } catch (error) {
    console.error('Database error:', error);
    return res.status(500).json({ 
      error: 'Internal server error' 
    });
  }
}
