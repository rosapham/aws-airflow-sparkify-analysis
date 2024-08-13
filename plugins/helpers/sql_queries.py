class SqlQueries:
    songplays_table_insert = """
        INSERT INTO songplays (start_time, userid, level, songid, artistid, sessionid,
            location, user_agent)
        SELECT DISTINCT TIMESTAMP 'epoch' + ts/1000 * interval '1 second' as start_time,
                userId,
                level,
                song_id as songid,
                artist_id as artistid,
                sessionId,
                location,
                userAgent as user_agent
        FROM staging_events se
        JOIN staging_songs ss
        ON se.song = ss.title AND se.artist = ss.artist_name AND se.length = ss.duration
        WHERE page = 'NextSong' AND userId IS NOT NULL AND songid IS NOT NULL
            AND artistid IS NOT NULL AND ts IS NOT NULL
    """

    user_table_insert = """
        SELECT distinct userid, firstname, lastname, gender, level
        FROM staging_events
        WHERE page='NextSong'
    """

    song_table_insert = """
        SELECT distinct song_id, title, artist_id, year, duration
        FROM staging_songs
    """

    artist_table_insert = """
        SELECT distinct artist_id, artist_name, artist_location, artist_latitude, artist_longitude
        FROM staging_songs
    """

    time_table_insert = """
        SELECT start_time, extract(hour from start_time),
                extract(day from start_time), extract(week from start_time),
                extract(month from start_time), extract(year from start_time),
                extract(dayofweek from start_time)
        FROM songplays
    """

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

    staging_songs_table_create = """
    CREATE TABLE public.staging_songs (
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

    artist_table_create = """
        CREATE TABLE public.artists (
        artistid varchar(256) NOT NULL,
        name varchar(512),
        location varchar(512),
        lattitude numeric(18,0),
        longitude numeric(18,0)
    );
    """

    songplays_table_create = """
        CREATE TABLE IF NOT EXISTS public.songplays (
        playid integer identity(0,1) primary key,
        start_time timestamp NOT NULL,
        userid int4 NOT NULL,
        level varchar(256),
        songid varchar(256),
        artistid varchar(256),
        sessionid int4,
        location varchar(256),
        user_agent varchar(256)
    );
    """

    songs_table_create = """
        CREATE TABLE public.songs (
        songid varchar(256) NOT NULL,
        title varchar(512),
        artistid varchar(256),
        'year' int4,
        duration numeric(18,0),
        CONSTRAINT songs_pkey PRIMARY KEY (songid)
    );
    """

    time_table_create = """
    CREATE TABLE public.'time' (
        start_time timestamp NOT NULL,
        'hour' int4,
        'day' int4,
        week int4,
        'month' varchar(256),
        'year' int4,
        weekday varchar(256),
        CONSTRAINT time_pkey PRIMARY KEY (start_time)
    ) ;
    """

    users_table_create = """
        CREATE TABLE public.users (
        userid int4 NOT NULL,
        first_name varchar(256),
        last_name varchar(256),
        gender varchar(256),
        'level' varchar(256),
        CONSTRAINT users_pkey PRIMARY KEY (userid)
    );
    """
