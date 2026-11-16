# Project report
All final query outputs, tables, streaming metrics, etc.

**Required Methodology Write-up:** For each major section (Preparation, Exploration, Enrichment, Streaming), you must include a dedicated Methodological Explanation covering:
- The methodology and why it was chosen
- Alternatives considered and trade-offs made

## Preparation

## Exploration

All results below are produced by `src/02_explore.py` (full output in `reports/02_exploration.txt`).

```text
Feature-length movies: 490,270
(movie, genre) rows:   762,427
```

All questions start from the cleaned Parquet files produced in the Preparation step. We build two base DataFrames that are reused by almost every question :
- movies : titles with titleType = movie and runtimeMinutes >= 40 (the AMPAS / BFI definition of a feature film), left-joined with title.ratings so that unrated movies are kept.
- movie_genres : the same movies with the genres array exploded, so we get one row per (movie, genre). A movie with three genres counts once in each of them.

Both DataFrames are cached because they are read by several questions, which avoids re-reading and re-joining the Parquet files each time. Each question is written as its own function, which keeps the script readable and makes it easy to run or debug one question at a time.

We used the DataFrame API (pyspark.sql.functions and Window) everywhere rather than RDDs. 
Spark cannot optimize RDD code, because it doesn’t know what our functions do. DataFrame code is automatically optimized by Spark (Catalyst optimizer).

All thresholds (minimum votes, rating cut-offs, etc.) are defined as constants, so they can be changed without touching the query logic.


### Q1. Unusual runtimes by genre

> Find all feature-length movies by genre whose length is notably different than the average length of movies in that genre.

**Results**

```text
Outliers (|z| > 2): 13,701
      genre  outlier_count  avg_runtime
   Thriller           2099         96.1
    Romance           2048         99.1
      Drama           1971         96.1
     Comedy           1621         93.1
      Crime           1189         95.6
     Action            843        101.2
     Horror            711         88.6
  Adventure            634         93.0
  Biography            470         89.2
     Family            282         92.4
    Musical            240        100.0
    Fantasy            239         93.9
        War            217         94.7
  Animation            175         82.7
    History            152         94.3
    Western            151         76.5
     Sci-Fi            147         91.2
      Adult            139         79.5
      Sport            100         88.9
      Music             97         88.4
Documentary             58         78.0
    Mystery             52         94.4
  Film-Noir             31         81.9
       News             27         77.6
  Talk-Show              5         87.6
 Reality-TV              2         92.2
  Game-Show              1         80.5

Most extreme examples:
                                   primaryTitle  startYear       genre  runtimeMinutes     z
                                      Logistics       2012 Documentary           51420 230.0
                                       Ambiancé       2020 Documentary           43200 193.1
                                     London EC1       2015      Comedy            5460 187.1
                                        Hunger!       2015       Drama            6000 159.4
                                    Matrjoschka       2006       Drama            5700 151.3
                                     London EC1       2015       Drama            5460 144.8
                Carnets Filmés (Liste Complète)       2019 Documentary           28643 127.9
                                     London EC1       2015     Mystery            5460 106.6
                          The Cure for Insomnia       1987       Music            5220  89.0
                          A 2nd generation film       2013       Drama            3077  80.5
The Longest Most Meaningless Movie in the World       1970       Drama            2880  75.1
                       World Peace & Prayer Day       2011      Family            2400  70.6
                           Modern Times Forever       2011 Documentary           14400  64.1
                                           h36:       2017      Action            2160  61.0
                                 24 Hour Psycho       1993      Horror            1440  60.6
```

13 701 (movie, genre) pairs have a runtime more than 2 standard deviations away from their genre average. The genres with the most outliers seem to be the most well-known genres (Thriller, Romance, Drama, Comedy). The most extreme cases are experimental or art films that are days long (Logistics runs 857 hours, Ambiancé 720 hours). These are real films, not data errors, so we kept them. 

Note that one movie can appear several times (e.g. London EC1 in comedy, drama and mystery) because it is compared to each of its genres.


**Methodology**

For each genre we compute the average and the standard deviation of runtimeMinutes with a groupBy("genre"). We join these statistics back to movie_genres and compute a z-score for each movie : `z = (runtime - genre_avg) / genre_std`

A movie is notably different if its runtime is far from the usual runtime of its genre. To measure "far", we use the z-score : it tells how many "typical gaps" a movie is from the genre average. We flag movies more than 2 typical gaps away, which is a common rule (only about 5% of movies normally go that far for a normal distribution).

We chose this method because each genre is compared to itself. For example, 150 minutes is normal for a drama but very long for an animation movie. 


**Alternatives considered and trade-offs**

- IQR method or median : these are more robust because the mean and standard deviation are themselves pulled up by the extreme films (a single 51 420 minutes movie inflates the Documentary std a lot). 
With a robust method, more "moderately long" films would be flagged. We kept the z-score because it is simple and easy to explain, but this is the main weakness of our approach.
- Fixed thresholds (e.g. < 60 min or > 180 min) : simpler, but ignores the differences between genres.


### Q2. Top three genres per year

> What are the top three most popular movie genres for each year between 1980 and 2025?

**Results**

```text
 startYear     genre  total_votes  movie_count  rank
      1980     Drama      3166392         1332     1
      1980 Adventure      2483466          163     2
      1980    Horror      2075199          151     3
      1981 Adventure      2252072          177     1
      1981     Drama      1712819         1282     2
      1981    Horror      1277521          169     3
      1982     Drama      3076111         1362     1
      1982    Sci-Fi      2460120           55     2
      1982    Action      1874767          334     3
      1983     Drama      2923431         1406     1
      1983    Action      2025900          340     2
      1983 Adventure      1985754          170     3
      1984    Action      3670441          323     1
      1984     Drama      3026182         1379     2
      1984 Adventure      2933748          161     3
      1985    Comedy      4568041          688     1
      1985 Adventure      3413554          167     2
      1985     Drama      3080089         1332     3
      1986     Drama      4001952         1434     1
      1986 Adventure      2967730          178     2
      1986    Comedy      2788343          700     3
      1987     Drama      4139914         1545     1
      1987    Comedy      3690085          701     2
      1987    Action      2362674          477     3
      1988     Drama      4468508         1433     1
      1988    Comedy      3991002          687     2
      1988    Action      2716655          513     3
      1989    Comedy      5016649          771     1
      1989     Drama      3964976         1457     2
      1989 Adventure      2945565          181     3
      1990     Drama      6327138         1451     1
      1990    Comedy      4019368          722     2
      1990     Crime      3591429          403     3
      1991     Drama      5590382         1430     1
      1991     Crime      3677458          314     2
      1991    Comedy      3197117          704     3
      1992     Drama      6101765         1332     1
      1992    Comedy      4076942          701     2
      1992     Crime      3612529          321     3
      1993     Drama      9002975         1277     1
      1993    Comedy      4503046          627     2
      1993     Crime      3048857          333     3
      1994     Drama     15752736         1339     1
      1994     Crime      6509651          308     2
      1994    Comedy      5681665          683     3
      1995     Drama     12533631         1366     1
      1995     Crime      7771524          293     2
      1995    Comedy      6086183          656     3
      1996     Drama      7866972         1348     1
      1996    Comedy      4792857          727     2
      1996     Crime      4448022          275     3
      1997     Drama     12639333         1480     1
      1997    Action      6772493          400     2
      1997    Comedy      5608908          789     3
      1998     Drama     11790957         1462     1
      1998    Comedy      9869650          827     2
      1998     Crime      6222450          327     3
      1999     Drama     15534440         1575     1
      1999    Comedy      8227812          880     2
      1999     Crime      7187664          325     3
      2000     Drama     13031917         1684     1
      2000    Comedy      8340579          890     2
      2000 Adventure      6996013          153     3
      2001     Drama     14747202         1829     1
      2001    Comedy      9519374          969     2
      2001 Adventure      9260197          166     3
      2002     Drama     17072300         1919     1
      2002 Adventure      8699019          171     2
      2002    Action      8400360          478     3
      2003     Drama     13265093         2020     1
      2003    Action     11717322          472     2
      2003    Comedy     10766208         1142     3
      2004     Drama     16483963         2104     1
      2004    Action     11527898          497     2
      2004    Comedy     10954974         1161     3
      2005     Drama     13698436         2368     1
      2005    Comedy     10199108         1294     2
      2005    Action      9065615          459     3
      2006     Drama     20898007         2603     1
      2006    Comedy     11782897         1434     2
      2006  Thriller      8670532          465     3
      2007     Drama     18546296         2803     1
      2007    Comedy     11723268         1451     2
      2007    Action      8999236          513     3
      2008     Drama     17584314         3134     1
      2008    Action     13415714          575     2
      2008    Comedy     11334272         1657     3
      2009     Drama     17601805         3698     1
      2009    Comedy     13470831         1948     2
      2009    Action     12123001          702     3
      2010     Drama     17563786         4158     1
      2010    Action     13341313          854     2
      2010 Adventure     13187643          504     3
      2011     Drama     20756863         4340     1
      2011    Action     17156057          858     2
      2011    Comedy     13988037         2201     3
      2012     Drama     20125983         4643     1
      2012    Action     14903073          860     2
      2012    Comedy     12515795         2344     3
      2013     Drama     21995304         5153     1
      2013    Action     17194791          967     2
      2013    Comedy     14963722         2550     3
      2014     Drama     24529346         5368     1
      2014    Action     20948069         1017     2
      2014 Adventure     17751200          789     3
      2015     Drama     20501299         5443     1
      2015    Action     15570179         1048     2
      2015 Adventure     14171450          696     3
      2016     Drama     20898937         5459     1
      2016    Action     17217420         1123     2
      2016 Adventure     12370868          694     3
      2017     Drama     20817173         5574     1
      2017    Action     16718075         1155     2
      2017    Comedy     11311032         2700     3
      2018     Drama     17421223         5641     1
      2018    Action     15088413         1107     2
      2018 Adventure     13356640          665     3
      2019     Drama     22037565         5592     1
      2019    Action     12368667         1133     2
      2019    Comedy     12274001         2635     3
      2020     Drama     10484056         4269     1
      2020    Comedy      6519042         1976     2
      2020    Action      6134794          828     3
      2021     Drama     14407926         4956     1
      2021    Action     12882768         1091     2
      2021 Adventure     10197209          558     3
      2022     Drama     15981356         5546     1
      2022    Action     13363000         1169     2
      2022    Comedy     10211353         2794     3
      2023     Drama     14779155         5757     1
      2023    Action     10592990         1287     2
      2023    Comedy      9781075         2883     3
      2024     Drama     11684396         5563     1
      2024    Action      8660597         1289     2
      2024    Comedy      7102328         2771     3
      2025     Drama      8665605         5129     1
      2025    Action      7707374         1285     2
      2025    Comedy      5919816         2571     3
```

Drama is first almost every year. The second and third places are mostly shared between Comedy, Action, Adventure and Crime. 
 
**Methodology**
 
We defined "popularity" as the total number of IMDb votes received by the movies of a genre in a given year. 
The number of votes measures how many people watched and cared enough to rate a film, which is closer to "popular" than the rating itself (a 9/10 film with 50 votes is not popular).
 
Steps :
 
1. Filter `movie_genres` on `startYear` between 1980 and 2025.
2. `groupBy("startYear", "genre")` and compute `sum(numVotes)` and `count(*)`.
3. Rank genres inside each year with `row_number()` over a window partitioned by `startYear` and ordered by `total_votes` descending.
4. Keep `rank <= 3`.

A window function is the natural tool for "top N per group" in Spark, because it ranks rows inside each partition without collecting anything on the driver. The movie count is kept as an extra column to help interpret the results.
 
**Alternatives considered and trade-offs**
 
- **Number of movies per genre** : would mostly reflect production volume, and the most common genres would always win since they would be more movies produced. It measures what is produced, not what people watch.
- **Average rating** : measures quality, not popularity, and is very sensitive to small genres with a few highly-rated films.


### Q3. Top three genres compared with the rest

> Compare the results from the previous question. How do the top three genres compare to the rest of the genres of movies in a given year?

**Results**

```text
 startYear  top_vote_share_pct  top_avg_rating  rest_avg_rating
      1980                44.3            6.01             5.95
      1981                46.5            6.10             6.03
      1982                43.7            6.05             5.90
      1983                45.9            6.03             5.94
      1984                44.1            6.08             5.98
      1985                51.5            6.05             5.95
      1986                46.2            5.98             5.95
      1987                42.6            5.89             5.93
      1988                45.0            5.88             5.92
      1989                49.5            6.05             5.85
      1990                45.6            5.96             5.83
      1991                44.2            6.05             5.93
      1992                46.3            6.04             6.03
      1993                44.7            6.00             6.02
      1994                58.3            6.00             5.93
      1995                49.0            6.04             5.95
      1996                44.9            6.09             6.06
      1997                44.1            6.00             6.11
      1998                51.5            6.04             5.95
      1999                43.6            6.00             5.97
      2000                45.0            6.00             5.99
      2001                42.4            6.06             6.01
      2002                42.4            6.08             6.13
      2003                45.5            6.01             6.14
      2004                41.3            6.03             6.23
      2005                40.4            6.04             6.22
      2006                42.7            6.00             6.18
      2007                38.3            6.03             6.26
      2008                40.4            5.95             6.24
      2009                41.3            6.02             6.27
      2010                40.0            6.13             6.24
      2011                46.1            6.06             6.35
      2012                42.7            6.11             6.33
      2013                41.5            6.09             6.30
      2014                47.0            6.17             6.28
      2015                46.6            6.10             6.20
      2016                44.1            6.10             6.15
      2017                45.0            6.01             6.17
      2018                44.3            6.03             6.04
      2019                43.6            5.95             6.12
      2020                44.7            5.86             6.04
      2021                46.4            5.98             6.03
      2022                46.4            6.07             6.16
      2023                44.9            6.10             6.17
      2024                42.9            6.09             6.13
      2025                39.3            6.22             6.23

Overall 1980-2025:
 is_top  movie_genre_rows  total_votes  avg_rating
  False            348940   1727365482        6.14
   True            215495   1361120129        6.04
```

**Votes are very concentrated** : only 3 genres collect between 38% and 58% of all votes each year (around 44% on average). The peak is 1994 (58.3%), the year of *The Shawshank Redemption*, *Pulp Fiction* and *Forrest Gump*, which all belong to Drama and/or Crime.

**Popular does not mean better rated** : in most years until 2001 the top 3 genres have a slightly higher average rating than the other genres, but from 2002 onwards it is the opposite, the "rest" is rated higher every single year. Over the whole period the top genres average 6.04 against 6.14 for the rest.

The differences in rating are small (up to 0.3 points), so the main difference between the top 3 and the rest is probably in audience size, not in quality.


**Methodology**
 
We reused the top 3 DataFrame from Q2 as a lookup table : each (year, genre) in the top 3 gets a flag `is_top = True`. This table is left-joined to `movie_genres` (1980-2025) and missing flags are set to `False` with `coalesce`.
 
Then, for each year, we compute with conditional aggregations (`F.when(is_top, ...)` inside `sum` / `avg`) :
 
- the share of votes that goes to the top 3 genres,
- the average rating of movies in the top 3 genres,
- the average rating of movies in the other genres.

We also computed one global comparison over the whole period by grouping on `is_top` only. Conditional aggregation lets us get all metrics in a single `groupBy` pass instead of computing the top and the rest separately and joining them afterwards.
 
We compared both popularity (vote share) and quality (average rating) because the question is open, and these two dimensions give a different picture.
 
**Alternatives considered and trade-offs**
 
- **Comparing against each other genre individually** (e.g. rank 1 vs rank 10) : more detailed, but much harder to read over 46 years. Grouping into "top 3" vs "rest" gives a single, clear comparison per year.
- **Filtering out movies with few votes** : in this question we kept all rated movies, so many small films with few votes are included in the averages. A minimum vote threshold (like in Q4) would reduce noise, at the cost of ignoring a large part of the catalogue.


### Q4. One-hit directors

> List all directors who have directed exactly one movie with a rating >= 8.0 but who have multiple other movies directed with an average rating of <= 6.0.

**Results**

```text
Directors found: 168
           primaryName                                hit_title  hit_rating  other_count  other_avg
           Harish Vyas                               Om Ka Hari         9.8            2       5.75
António Borges Correia                  Antes de a Vida Começar         9.3            7       5.31
           Shin Jai-ho                                   Wanted         9.3            4       5.63
         Saurabh Varma                 The Great Punjab Robbery         9.2            4       5.65
       James Merendino       The Invisible Life of Thomas Lynch         9.1           10       5.30
            Paul Kampf                                  Harvest         9.1            2       5.05
        Alex R. Wagner Barbenheimer and the Cult of Walt Disney         9.1            6       5.93
             Tom Logan         Sean and Melissa: 10 Years Later         9.0            4       5.53
        R. Anantharaju                       Shreemati Sindoora         9.0            2       5.90
           Rabi Kinagi                             Suna Panjuri         9.0           23       5.77
      Shhivaji Doltade                                Raja Rani         9.0            2       4.80
         Anand Chandra                               Fight Maha         8.9            2       5.55
             Ray Brady                                Egression         8.8            7       5.47
       Vasudevan Sanal                                     Haya         8.8            2       5.90
    Carlos Etzio Roman                              Dissensions         8.7            2       5.95
          Rojin Thomas                                    #Home         8.7            2       5.95
    Thomas L. Phillips                            Rattle Basket         8.7            2       4.80
    Sam Logan Khaleghi                  Love Is a Thieves' Game         8.6            2       3.40
         Steve Rahaman                               To Be King         8.6            2       2.95
          David Leland                       Concert for George         8.6            5       5.64
```

We found 168 directors matching the criteria. Most of them are little-known directors whose "hit" seems to be a small or regional film with a few votes.
 
**Methodology**
 
1. Explode the `directors` array of `title.crew` to get one row per (movie, director).
2. Join with `movies`, keeping only movies with at least 100 votes, so that a film rated 10/10 by five people does not count as a hit.
3. For each director, compute in one `groupBy` with conditional aggregations :
   - `hit_count` : number of movies with rating >= 8.0,
   - `hit_title` / `hit_rating` : the title and rating of the hit,
   - `other_count` and `other_avg` : the number and average rating of the remaining movies.
4. Keep directors with `hit_count == 1`, `other_count >= 2` ("multiple" other movies) and `other_avg <= 6.0`.
5. Join with `name.basics` to get the director's name.
Doing everything in a single aggregation avoids splitting the data into "hits" and "others" and joining them back, which would add an extra shuffle.
 
**Alternatives considered and trade-offs**
 
- **Vote threshold**: 100 votes is a compromise. Without any threshold, the list is full of small movies with 5-10 votes and extreme ratings. With a higher threshold (e.g. 1,000 or 10,000 votes) the results would be more "famous" and more reliable, but many directors would lose their hit or drop below 2 other films, and the list would be much shorter. Ratings around 9+ with a few hundred votes are still suspicious (friends and family voting), which is a known limitation.
- **Using `title.principals` (category = director) instead of `title.crew`**: title.crew lists all credited directors, while title.principals only keeps the top-billed credits, so we used title.crew.

### Q5. TV series that declined

> Identify television series with at least three seasons consisting of >=10 episodes where the worst episode occurred in the last season and the best episode occurred within the first two seasons.

**Results**

```text
Series analysed: 6,041  |  matching: 601 (9.9%)
                     primaryTitle  season_count  best_season  worst_season
                    Papiny dochki            20            2            20
                Masterchef Italia            15            1            15
                         Roadkill            13            2            13
               Two and a Half Men            12            2            12
                   Evening Urgant            11            1            11
                     Murphy Brown            11            2            11
                      Disappeared            10            2            11
                        Home Town            10            2            10
                High Stakes Poker             9            1             9
                    The Virginian             9            2             9
                            Coach             9            2             9
                        The Flash             9            1             9
          The Beverly Hillbillies             9            2             9
                   The Cosby Show             8            2             8
                          Dragnet             8            2             8
Anthony Bourdain: No Reservations             8            2             9
             Catfish: The TV Show             8            2             8
                     All American             8            2             8
                            Weeds             8            2             8
                        Winx Club             8            2             8
```

Out of 6 041 series with at least 3 seasons of 10+ episodes, 601 (9.9%) follow this "declining" pattern. 
The list shows series that have lost quality over time. 
In some rows the number of seasons is smaller than the season number (e.g. *Disappeared*: 10 seasons, worst in season 11) because some seasons have fewer than 10 episodes and are not counted.
 
**Methodology**
 
1. From `title.episode`, drop episodes without a season number.
2. Count episodes per (series, season) and keep seasons with **>= 10 episodes**.
3. Count those qualifying seasons per series and keep series with **>= 3 of them**. The "last season" is the highest season number among the qualifying seasons.
4. Join the episodes of these seasons with `title.ratings` (episodes without rating are dropped).
5. Use two window functions partitioned by series:
   - one ordered by rating ascending → rank 1 is the worst episode,
   - one ordered by rating descending → rank 1 is the best episode.
   Ties on rating are broken by the number of votes (the episode with the most votes wins), so the result is deterministic.
6. Keep series where the worst episode is in the last season and the best episode is in season 1 or 2.
Window functions with `row_number()` are a clean way to find the "argmin / argmax" row of each group in Spark without collecting data on the driver.
 
**Alternatives considered and trade-offs**
 
- **Season average instead of a single episode**: a single episode can be an outlier (e.g. a controversial episode, or a clip show). Comparing average ratings per season would measure the decline of the show more robustly, but it is not what the question asks.
- **Definition of "last season"**: we used the last season with >= 10 episodes. Another option would be the real final season of the show, even if it is short. We chose the first option because the question focuses on seasons with >= 10 episodes, and a 3-episode final season would not be comparable.


### Q6. Success across languages

> Find any actors/actresses who have achieved success across films in multiple distinct languages (e.g. been in a successful film originally in French and English). Additionally, list all actors/actresses who have achieved success in movies in >=3 languages (e.g. French, German, and English).

**Results**

```text
Successful movies: 4,974  |  with a known language: 4,716 (94.8%)
Actors with >= 2 languages: 1,199
Actors with >= 3 languages: 170
        primaryName  language_count  hit_count          languages
          Şener Şen               5         14 de, en, fr, ja, tr
       Song Kang-ho               5         11 en, es, fr, it, ko
Tony Leung Chiu-wai               5         10 de, en, es, fr, ko
     Toshirō Mifune               5          6 de, en, fr, it, sv
      Max von Sydow               4         17     da, en, fr, sv
   Gérard Depardieu               4         12     en, es, fr, it
         Cem Yılmaz               4          8     de, en, ja, tr
 Thomas Kretschmann               4          8     da, de, en, ko
      Maggie Cheung               4          7    en, es, ko, yue
        Liv Ullmann               4          7     da, en, fr, sv
     Bibi Andersson               4          7     da, en, fr, sv
   Christian Berkel               4          7    da, de, en, yue
      Trine Dyrholm               4          7     da, en, fr, it
    Tatsuya Nakadai               4          7     de, en, fr, sv
    Ryu Seung-ryong               4          5     de, en, it, ko
    Alba Rohrwacher               4          5     en, es, fr, it
  Takeshi Kaneshiro               4          5    ca, de, fr, yue
       Ma Dong-seok               4          4     de, es, fr, ko
   Ryūnosuke Kamiki               4          4     de, en, fr, ja
     Robert De Niro               3         34         en, es, it
```

We found 1 199 actors with successful films in at least 2 languages and 170 in at least 3. 
 
**Methodology**
 
**Defining success** : a movie is successful if its rating is >= 7.0 and it has >= 10 000 votes. The vote threshold makes sure the film was actually seen by a large audience, and the rating ensures it was well received.
 
**Finding the original language** : the IMDb dataset has no "original language" column, so we had to estimate it. In `title.akas`, each title has several alternative names, each with a region and sometimes a language. Our approach :
 
1. Keep the akas whose title is equal (case-insensitive) to the movie's `originalTitle`, i.e. the versions that use the original name.
2. Take the aka's `language` if it is filled, otherwise convert its `region` to the main language of that country with a small mapping dictionary (US → en, FR → fr, JP → ja, ...), implemented as a Spark map column with `create_map` (so no Python UDF is needed).
3. Count how many times each language appears per movie and keep the most frequent one using a window with `row_number()`.

This gives a language for 94.8% of the successful movies. The other 5.2% are dropped (inner join), so they don’t count toward any actor.
 
**Counting per actor** : we join `title.principals` (category actor / actress) with the successful movies and their language, then group by actor and count distinct languages with `countDistinct`. Actors with >= 2 and >= 3 languages answer the two parts of the question.
 
**Alternatives considered and trade-offs**
 
- **External source** (Wikidata, TMDB `original_language`) : this would give the real original language and would be much more accurate. It would require enrichment from an open source with lineage, as in the Enrichment section. We stayed with IMDb-only data for this exploration step, but this would be the first improvement to make.
- **Success threshold**: 7.0 / 10,000 votes is subjective. A lower vote threshold would include more non-English films (which tend to have fewer IMDb votes), but would also include more unknown titles.


### Q7. Early career peak

> Find all actors/actresses who have been in movies for 20+ years and at least 15 or more movies whose popularity peaked early in their career (e.g. had their highest rated movies early in their career as compared to later.)

**Results**

```text
1,136 of 3,528 eligible actors (32.2 %) peaked early
        primaryName  movie_count  first_year  last_year  early_avg  late_avg  drop               best_title
     Halit Akçatepe           24        1972       2005       7.63      3.45  4.18          The Chaos Class
      Wendell Corey           22        1947       1968       7.01      3.65  3.36              Rear Window
       Cüneyt Arkın           18        1964       2014       6.34      3.50  2.84                 The Mine
       Evelyn Keyes           17        1939       1989       6.97      4.15  2.82       Gone with the Wind
         Madhan Bob           16        1992       2021       7.52      4.83  2.69             Thevar Magan
       Ranjeet Bedi           17        1979       2017       6.72      4.08  2.64           Satte Pe Satta
Subhalekha Sudhakar           23        1987       2026       8.38      5.92  2.46         Aha Naa Pellanta
        James Duval           22        1993       2024       6.11      3.79  2.32             Donnie Darko
       Steve Brodie           20        1947       1988       6.44      4.13  2.31          Out of the Past
     Rajendra Gupta           18        2000       2025       7.34      5.06  2.28         Paan Singh Tomar
     Baiju Santhosh           18        1989       2026       8.30      6.06  2.24      Vadakkunokkiyantram
      Corey Feldman           15        1984       2021       6.26      4.05  2.21              Stand by Me
             Tulasi           25        1980       2026       8.70      6.50  2.20          Sankarabharanam
      Metin Akpınar           19        1973       2018       7.28      5.10  2.18          My Dear Brother
             Geetha           17        1983       2025       7.93      5.75  2.18          Sagara Sangamam
        Max Perlich           19        1989       2020       5.87      3.70  2.17         Drugstore Cowboy
     Robert LaSardo           24        1991       2024       6.10      3.95  2.15          Out for Justice
      Andrew Duggan           16        1957       1987       6.48      4.35  2.13        Seven Days in May
     Andrew Stevens           16        1976       2022       5.43      3.30  2.13               Death Hunt
   Mehmet Ali Erbil           15        1981       2024       5.08      2.96  2.12 The Chaos Class: Bye Bye
```

About one third of the eligible actors (1,136 out of 3,528) peaked early. 

**Methodology**
 
1. Join actors / actresses from `title.principals` with `movies`, keeping only movies with >= 1,000 votes and a known year (to avoid noisy ratings and very obscure titles).
2. With window functions partitioned by actor, compute the first and last year of the career and find the best-rated movie (ties broken by number of votes).
3. Split each career in two halves at the middle year `(first_year + last_year) / 2` : movies before or at the middle are "early", the others are "late".
4. Group by actor to compute the number of movies, the early and late average ratings, and the year and title of the best movie.
5. Keep eligible actors: career span >= 20 years and >= 15 movies. Since only films with >= 1,000 votes are counted, this is the actor’s visible career, not their full filmography.
6. An actor "peaked early" if both conditions hold :
   - the early average is at least 0.5 points higher than the late average,
   - the best-rated movie is in the first half of the career.
Using both conditions makes the result more robust : the best movie alone could be a single lucky film, and the averages alone could hide where the actual peak was.
 
**Alternatives considered and trade-offs**
 
- **Splitting by number of movies instead of years** (first half of the filmography vs second half) : would give two groups of the same size, but an actor who made 20 films in 5 years and then a few films over 20 years would have a strange split. We chose years because the question talks about career in time.
- **Popularity as votes instead of rating** : the question says "popularity", but the example mentions "highest rated movies", so we used the rating. Using votes would be biased towards recent films, since older films collected fewer IMDb votes.
- **Thresholds** : the 0.5 point drop and 1,000 votes are our own choices. A larger drop gives fewer but clearer cases. Note also that the data contains some 2026 titles (announced or recently released movies), which slightly extends some careers.


### Q8. Multi-talented professionals

> Identify film professionals who can be considered "multi-talented" (e.g. some combo of writer/director/actor). How do these individuals perform when performing multiple roles on the same project (e.g. directing and acting in the same movie)?

**Results**

```text
Multi-talented people: 3,654
              talents  count
      director+writer   2670
actor+director+writer    440
         actor+writer    392
       actor+director    152

Ratings by role combination on the same movie:
                combo  movie_roles  avg_rating  avg_votes
      director+writer        14686        6.42      35983
                actor        11010        6.24      58309
             director         5572        6.31      51450
               writer         5307        6.24      65545
actor+director+writer         1506        6.21      22739
         actor+writer         1374        6.14      43048
       actor+director          342        6.27      44319

Same person, multi-role vs single-role movies: 2,735 people
Better rated when holding several roles: 1,564 (57.2 %)  |  average gain: +0.11
       primaryName               talents  multi_movies  multi_avg  single_avg  gain
       Woody Allen actor+director+writer            54       6.89        5.87  1.02
     Werner Herzog actor+director+writer            41       7.11        6.47  0.64
    Claude Chabrol actor+director+writer            37       6.82        6.75  0.07
    Ingmar Bergman actor+director+writer            36       7.33        7.20  0.13
   Jean-Luc Godard actor+director+writer            35       6.65        6.22  0.43
       Tyler Perry actor+director+writer            35       5.36        6.43 -1.07
    Akira Kurosawa       director+writer            31       7.54        6.96  0.58
      Jesús Franco actor+director+writer            30       4.86        4.54  0.32
      Satyajit Ray       director+writer            28       7.93        7.05  0.88
       Wim Wenders       director+writer            27       7.00        6.41  0.59
    Clint Eastwood        actor+director            26       6.89        6.90 -0.01
      Billy Wilder       director+writer            26       7.38        6.98  0.40
       Mani Ratnam       director+writer            25       7.41        6.33  1.08
Sylvester Stallone actor+director+writer            25       6.32        5.61  0.71
      Kamal Haasan actor+director+writer            25       7.51        7.69 -0.18
       Éric Rohmer       director+writer            25       7.15        5.50  1.65
       Luis Buñuel       director+writer            25       7.36        7.27  0.09
      Yasujirō Ozu       director+writer            25       7.62        7.00  0.62
         Jing Wong actor+director+writer            24       6.12        6.56 -0.44
      Priyadarshan       director+writer            24       6.86        7.12 -0.26
```

We found 3,654 multi-talented people. The large majority (2,670) are director-writers. Only 440 people combine all three roles.

Writing and directing the same film works best : director+writer movies have the highest average rating (6.42), above movies where these people only direct (6.31) or only write or act (6.24). These averages only cover the multi-talented people, not all directors, writers or actors.

Acting in your own film does not help much : as soon as acting is added to the combination, the average rating goes down (actor+writer 6.14, actor+director+writer 6.21).

Among the 2,735 people who have both kinds of movies, 57.2% get better ratings on the films where they hold several roles, with an average gain of +0.11. So there is a small positive effect, but it is not huge.
Individual cases vary a lot: Woody Allen (+1.02) and Éric Rohmer (+1.65) are much better rated on their personal projects, while Tyler Perry (-1.07) is rated better when he only holds one role.


**Methodology**
 
1. Build a single "roles" table with one row per (movie, person, role) :
   - directors and writers from `title.crew` (exploded arrays),
   - actors / actresses from `title.principals`,
   combined with `unionByName` and deduplicated with `distinct()`. We only keep movies with >= 1,000 votes.
2. A person has a talent for a role if they held it in >= 3 movies. A person is multi-talented if they have at least 2 such talents. This avoids counting, for example, a director who once made a cameo in their own movie as an actor.
3. For each multi-talented person and each movie, we group their roles to get the combination they held on that movie (e.g. `actor+director`), sorted alphabetically with `array_sort` so that the label is always the same.
4. We compare ratings in two ways:
   - per combination : average rating and votes for each role combination,
   - per person : for each individual, average rating of their multi-role movies vs their single-role movies. This second comparison is the most interesting because it compares people with themselves, so it removes the effect of individual talent.
Building one unified roles table with a union made it easy to treat the three roles the same way, instead of writing separate joins for each combination.
 
**Alternatives considered and trade-offs**
 
- **Including other roles** (producer, composer, casting_director, ...) : `title.principals` contains these categories, and including them would give a broader definition of multi-talented. We kept the three roles given as examples in the question to keep the analysis focused.
- **Selection bias** : people usually take several roles on projects that are personal to them, which may be different in nature (smaller budget, more artistic). So the rating difference cannot be read as "holding several roles causes better films", only as a correlation.


## Enrichment

## Streaming


