class SqlQueries:

    # Create staging_events table
    staging_events_table_create = """
        CREATE TABLE IF NOT EXISTS public.staging_events (
        artist varchar(256),
        auth varchar(256),
        firstname varchar(256),
        gender varchar(256),
        iteminsession int4,
        lastname varchar(256),
        length numeric(18,0),
        level varchar(256),
        location varchar(256),
        method varchar(256),
        page varchar(256),
        registration numeric(18,0),
        sessionid int4,
        song varchar(256),
        status int4,
        ts int8,
        useragent varchar(256),
        userid int4
    );
    """

    # Create staging_songs table
    staging_songs_table_create = """
    CREATE TABLE IF NOT EXISTS public.staging_songs (
        num_songs int4,
        artist_id varchar(256),
        artist_name varchar(512),
        artist_latitude numeric(18,0),
        artist_longitude numeric(18,0),
        artist_location varchar(512),
        song_id varchar(256),
        title varchar(512),
        duration numeric(18,0),
        year int4
    );
    """

    # Create songplays table
    songplays_table_create = """
        CREATE TABLE IF NOT EXISTS public.songplays (
        playid varchar(256) primary key,
        start_time timestamp NOT NULL sortkey distkey,
        userid int4 NOT NULL,
        level varchar(256),
        songid varchar(256),
        artistid varchar(256),
        sessionid int4,
        location varchar(256),
        user_agent varchar(256)
    );
    """

    # Insert values from staging_events and staging_songs into songplays table
    songplays_table_insert = """
        INSERT INTO songplays (playid, start_time, userid, level, songid, artistid, sessionid,
            location, user_agent)
        SELECT DISTINCT
                md5(se.sessionid || se.start_time) as playid,
                se.start_time,
                se.userId,
                se.level,
                ss.song_id as songid,
                ss.artist_id as artistid,
                se.sessionId,
                se.location,
                se.userAgent as user_agent
        FROM (SELECT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' AS start_time, *
            FROM staging_events
            WHERE page='NextSong') se
        JOIN staging_songs ss
        ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration
    """

    # Create users table
    users_table_create = """
        CREATE TABLE IF NOT EXISTS public.users (
        userid int4 NOT NULL sortkey,
        first_name varchar(256),
        last_name varchar(256),
        gender varchar(256),
        level varchar(256),
        CONSTRAINT users_pkey PRIMARY KEY (userid)
    );
    """

    # Insert values from staging_events into users table
    users_table_insert = """
        INSERT INTO users (userid, first_name, last_name, gender, level)
            SELECT DISTINCT userId,
                    firstName as first_name,
                    lastName as last_name,
                    gender,
                    level
            FROM staging_events
            WHERE page = 'NextSong' AND userId IS NOT NULL
    """

    # Create songs table
    songs_table_create = """
        CREATE TABLE IF NOT EXISTS public.songs (
        songid varchar(256) NOT NULL sortkey,
        title varchar(512),
        artistid varchar(256),
        year int4,
        duration numeric(18,0),
        CONSTRAINT songs_pkey PRIMARY KEY (songid)
    );
    """

    # Insert values from staging_songs into songs table
    songs_table_insert = """
        INSERT INTO songs (songid, title, artistid, year, duration)
            SELECT DISTINCT song_id as songid,
                    title,
                    artist_id as artistid,
                    year,
                    duration
            FROM staging_songs
            WHERE songid IS NOT NULL
    """

    # Create artists table
    artists_table_create = """
        CREATE TABLE IF NOT EXISTS public.artists (
        artistid varchar(256) NOT NULL sortkey,
        name varchar(512),
        location varchar(512),
        latitude numeric(18,0),
        longitude numeric(18,0)
    );
    """

    # Insert values from staging_songs into artists table
    artists_table_insert = """
        INSERT INTO artists (artistid, name, location, latitude, longitude)
            SELECT DISTINCT artist_id as artistid,
                    artist_name as name,
                    artist_location as location,
                    artist_latitude as latitude,
                    artist_longitude as longitude
            FROM staging_songs
            WHERE artist_id IS NOT NULL
    """

    # Create time table
    time_table_create = """
        CREATE TABLE IF NOT EXISTS public.time (
            start_time timestamp NOT NULL sortkey,
            hour int4,
            day int4,
            week int4,
            month varchar(256),
            year int4,
            weekday varchar(256),
            CONSTRAINT time_pkey PRIMARY KEY (start_time)
    ) ;
    """

    # Insert values from staging_events into time table
    time_table_insert = """
        INSERT INTO time (start_time, hour, day, week, month, year, weekday)
            SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as start_time,
                    EXTRACT(hour FROM start_time) as hour,
                    EXTRACT(day FROM start_time) as day,
                    EXTRACT(week FROM start_time) as week,
                    EXTRACT(month FROM start_time) as month,
                    EXTRACT(year FROM start_time) as year,
                    EXTRACT(weekday FROM start_time) as weekday
            FROM staging_events
            WHERE page = 'NextSong' AND ts IS NOT NULL
    """
