-- Query 1: Get all columns from the dataset
SELECT * FROM table_name;

-- Query 2: Get tracks with a specific genre
SELECT track_name, genre FROM table_name WHERE genre = 'Pop';

-- Query 3: Get all tracks released after a specific date
SELECT track_name, release_date FROM table_name WHERE release_date > '2020-01-01';

-- Query 4: Count the number of tracks for each album
SELECT album_name, COUNT(track_id) AS track_count FROM table_name GROUP BY album_name;

-- Query 5: Find the average duration of tracks (in seconds)
SELECT AVG(duration_sec) AS avg_duration_sec FROM table_name;

-- Query 6: Get the total number of followers for each artist
SELECT artist_name, SUM(followers) AS total_followers FROM table_name GROUP BY artist_name;

-- Query 7: Get the top 10 most popular tracks
SELECT track_name, track_popularity FROM table_name ORDER BY track_popularity DESC LIMIT 10;

-- Query 8: Get the tracks with the longest duration
SELECT track_name, duration_sec FROM table_name ORDER BY duration_sec DESC LIMIT 10;

-- Query 9: Join tracks with their corresponding album information
SELECT t.track_name, a.album_name
FROM table_name t
JOIN albums a ON t.album_id = a.album_id;

-- Query 10: Join tracks with their artist information
SELECT t.track_name, ar.artist_name
FROM table_name t
JOIN artists ar ON t.artist_id = ar.artist_id;

-- Query 11: Get the average track popularity by genre
SELECT genre, AVG(track_popularity) AS avg_popularity FROM table_name GROUP BY genre;

-- Query 12: Get the total number of tracks for each artist and their popularity
SELECT artist_name, COUNT(track_id) AS track_count, AVG(track_popularity) AS avg_popularity
FROM table_name
GROUP BY artist_name;

-- Query 13: Find the most popular artist (by followers)
SELECT artist_name, MAX(followers) AS most_followed
FROM table_name
GROUP BY artist_name;

-- Query 14: Get the track with the highest popularity
SELECT track_name, track_popularity
FROM table_name
ORDER BY track_popularity DESC LIMIT 1;

-- Query 15: Find tracks with durations greater than the average
SELECT track_name, duration_sec
FROM table_name
WHERE duration_sec > (SELECT AVG(duration_sec) FROM table_name);

-- Query 16: Get the artist with the highest average track popularity
SELECT artist_name, AVG(track_popularity) AS avg_popularity
FROM table_name
GROUP BY artist_name
ORDER BY avg_popularity DESC LIMIT 1;
