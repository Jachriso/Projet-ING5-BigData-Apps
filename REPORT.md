# Project report
All final query outputs, tables, streaming metrics, etc.

**Required Methodology Write-up:** For each major section (Preparation, Exploration, Enrichment, Streaming), you must include a dedicated Methodological Explanation covering:
- The methodology and why it was chosen
- Alternatives considered and trade-offs made

## Preparation
Full output: [reports/01_data_preparation.txt](reports/01_data_preparation.txt)

Our cleaning script is divided in 5 different steps : 
1. Read the datasets in the `data/raw` folder
2. Report the null and missing values of each columns of each dataset
3. Common cleaning of the tables
4. Specific cleaning for  each tables
5. Save the tables in the `data/clean` folder 

Those steps enable us to answer the next 4 questions : 
1. For each column of each table in the IMDB dataset, calculate the percentage of nulls, missing, or empty records
2. Which five columns across the entire dataset have the highest percentage of null values?
3. Identify other data quality issues for the dataset such as principals who died before they were born, foreign keys with missing parent records, etc.
4. Design and implement a way to handle these values.

### Null, missing and empty values per column

| Table | Column | Missing values | Missing |
|------|------|---------:|---------:|
| name.basics | primaryName | 89 | 0.00 % |
| name.basics | birthYear | 15,126,718 | 95.66 % |
| name.basics | deathYear | 15,547,099 | 98.32 % |
| name.basics | primaryProfession | 3,219,851 | 20.36 % |
| name.basics | knownForTitles | 1,917,161 | 12.12 % |
| title.basics | startYear | 1,487,875 | 11.48 % |
| title.basics | endYear | 12,794,047 | 98.73 % |
| title.basics | runtimeMinutes | 8,056,989 | 62.18 % |
| title.basics | genres | 547,930 | 4.23 % |
| title.akas | region | 12,998,541 | 21.53 % |
| title.akas | language | 19,655,399 | 32.55 % |
| title.akas | types | 40,375,080 | 66.86 % |
| title.akas | attributes | 60,068,229 | 99.47 % |
| title.crew | directors | 5,789,625 | 44.68 % |
| title.crew | writers | 6,430,569 | 49.62 % |
| title.episode | seasonNumber | 2,123,916 | 21.17 % |
| title.episode | episodeNumber | 2,123,916 | 21.17 % |
| title.principals | job | 82,869,755 | 80.66 % |
| title.principals | characters | 52,680,715 | 51.28 % |

Here is a table with all columns which have null, missing or empty values. All other columns are complete.

### Top 5 columns with the most null values

| Rank | Column | Missing |
|------|------|---------|
| 1 | title.akas.attributes | 99.47 % |
| 2 | title.basics.endYear | 98.73 % |
| 3 | name.basics.deathYear | 98.32 % |
| 4 | name.basics.birthYear | 95.66 % |
| 5 | title.principals.job | 80.66 % |

Here is a list of the top 5 columns with the highest null, missing or empty values. 
We can divide the missing values in two categories : 
- Not applicable
- Information unknown.

For each of those column, we can identify the category of null values that we have : 
- for the column title.basics.endYear and name.basics.deathYear, we can explain that high percentage with the fact that people are not dead yet and that a movie has no end year and a series could still be on going. Which implies that we cannot apply a value in most cases.
- for the title.akas.attributes and title.principals.job columns, we can explain that high percentage because of the optional attribute of those columns. They are not mandatory information. For example, the job column is used to add information to the field category.
- for the name.basics.birthYear column, we could categorize it as unknown information because this dataset list all movie and series workers whether they are famous or not.

### Other data quality issues

**Primary keys and data types:** We didn't encounter any missing or duplicated primary key in any of the 7 tables, and no invalid number in the 13 numeric columns.

**Inconsistent values inside a table**

| Table | Issue | Rows | Share |
|------|------|---------:|---------:|
| name.basics | deathYear < birthYear | 28 | 0.0002 % |
| title.basics | runtimeMinutes <= 0 | 4 | 0.0000 % |
| title.basics | runtimeMinutes over 24 hours | 210 | 0.0016 % |
| title.basics | startYear > 2026 (announced titles) | 6,156 | 0.0475 % |

Here is a summary of the inconsistent issues encountered in the different tables. We didn't found any issues like a birthYear or deathYear in the future, an endYear inferior to a startYear, an endYear on a non-series title, a boolean value different than a 0 or a 1, episode being its own parent, negative season or episode number, averageRating outside 1-10, negative numVotes.

**Relations between tables**

| Check | Issues | Share |
|------|---------:|---------:|
| title.principals.nconst without parent in name.basics | 7,183 | 0.0070 % |
| title.crew.directors without parent in name.basics | 423 | 0.0044 % |
| title.crew.writers without parent in name.basics | 1,148 | 0.0075 % |
| title.episode.parentTconst without parent in title.basics | 5 | 0.0000 % |
| Principals (distinct people) who died before they were born | 20 | 0.0003 % |
| Principal credits on a title released before the person was born | 3,109 | 0.0081 % |

We didn't encounter any missing parent record for title.principals.tconst, title.ratings.tconst, title.crew.tconst, title.episode.tconst, title.akas.titleId and name.basics.knownForTitles. An all the relashionship issues affect only a small portion of the dataset. 

### Handling strategy

In our strategy, we decided to apply dropping rows only if they had a missing or a duplicated primary key value. After our analysis, we found out that none of our dataset rows had a missing or duplicated primary key value so no line was dropped. 
We decided to not drop rows for other missing values and decided to handle them differently : 

| Issue | Handling |
|------|------|
| deathYear < birthYear | both years set to null |
| runtimeMinutes <= 0 | set to null |
| runtimeMinutes > 1440 | kept |
| startYear > 2026 | kept (treated it as an announcment) |
| Missing parent records | kept, only counted (inner joins drop them later) |
| `\N` and empty strings | stored as null; other null values are kept |
| Numeric columns | cast with `try_cast` (invalid numbers become null) |
| List columns | split into arrays; `characters` parsed as JSON |

### Methodological explanation

#### Methodology and why it was chosen

To start cleaning our data, we started with calculating the number and percentage of missing / null values. Found each value, we analysed the results as stated in the answer to the question 2. We concluded that there were two types of missing and null values : the not applicable type and the unknown type. We then decided to treat those missing values without dropping any rows except for missing and duplicated primary key values. Our goal was to prevent the creation of more foreign keys with missing parent records. Concerning the foreign keys which already have missing parent records, we decided to keep them because they still had valid data. Also most of the time, it was missing additional data like the names of the actors, writers or directors, information which is not fundamentally necessary for every line. We also decided to treat incoherent data like explained in our handling strategy. 

#### Alternatives considered and trade-offs

At the beginning, when we had our first report on the missing values, we had the idea to drop the rows with a missing percentage when the column had a total percentage of missing and null values below 10%, replace the missing numerical values with 0 and change the endYear for non series to have endYear = startYear. 
But we cancelled all of those options because they would have resolved any issues of the dataset and only created new ones like new foreign keys missing parent records. 
So we changed strategies and instead of blindly changing the data, we tried to better understand the current data. for instance, replacing values by 0 could have been a huge mistake with for example the number of seasons because in this case, 0 means special seasons and episodes. Same for endYear and startYear : for a movie or a documentary for example, an endYear doesn't mean anything because it only has one year of release. 

## Exploration

All results below are produced by `src/02_explore.py` (full output in `reports/02_exploration.txt`).

```text
Feature-length movies: 490,270
(movie, genre) rows:   762,427
```

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

**Methodology**



**Alternatives considered and trade-offs**



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

**Methodology**



**Alternatives considered and trade-offs**



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

**Methodology**



**Alternatives considered and trade-offs**



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

**Methodology**



**Alternatives considered and trade-offs**



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

**Methodology**



**Alternatives considered and trade-offs**



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

**Methodology**



**Alternatives considered and trade-offs**



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

**Methodology**



**Alternatives considered and trade-offs**



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

**Methodology**



**Alternatives considered and trade-offs**


## Enrichment
Full output: [reports/03_enrichment.txt](reports/03_enrichment.txt)

### Scope

| Selection | Rows |
|------|------|
| Top 10 feature-length films per (genre, year), 1980-2025 | 10,198 (film, genre) |
| First 3 principals of each film | 21,490 (film, principal) |

To create our scope, we filtered the inner join of `title.basics` and `title.ratings` with the variables: `titleType = "movie"`, `FIRST_YEAR`, `LAST_YEAR`, `MIN_FEATURE_RUNTIME` (defined below). We set the minimum feature runtime to 40 minutes as it is the minimum runtime in the official definition of a feature-length film. After the filter, we partitioned our dataset and created a rank column to rank the movies by decreasing `numVotes` depending on the year and genre of the movie and we only took the `TOP_N_FILMS`. 

After obtaining our final movie dataframe, we filtered the inner join of its distinct movies with `title.principals` to obtain the first three principals for each movie. The join is done on distinct movies because a movie listed under two genres would otherwise have its principals duplicated. Another issue was that the same person could be credited several times on one movie, for example as both director and writer. So for each person we decided to keep their best position (minimum `ordering`), then take the first three distinct people. 

```
FIRST_YEAR, LAST_YEAR = 1980, 2025
MIN_FEATURE_RUNTIME = 40      # feature-length film, in minutes
TOP_N_FILMS = 10              # films per (genre, year), ranked by popularity (numVotes)
N_PRINCIPALS = 3              # principals enriched per film
```

### Sources and collection

To enrich our movies, we used two different sources: Wikidata and the English Wikipedia. Both are human-authored resources and have an open licence. We link our IMDb ids (`tconst` for the movies, `nconst` for the people) to Wikidata with the property P345 (IMDb id). We did not search by title because many movies share the same name or have remakes, so we could have matched the wrong page. Wikidata gives us the link to the English Wikipedia article. For the people, it also gives us the birth date, the death date, the birth place and the occupations.

Then we get the texts from Wikipedia:
- For a movie, the synopsis is the `Plot` section of the article. If there is no `Plot` section, we try in this order: `Plot summary`, `Synopsis`, `Premise` and `Story`. If none is found, we put the text to null and add a note to explain the reason it is empty.
- For a person, the biography is the introduction of the article. And if the information is missing or empty, we put the text to null and add a note to explain the reason it is empty.

We decided to do this part in Python and not in Spark, because it is only a series of HTTP requests with a rate limit, and Spark cannot make it faster. To reduce the number of requests, we grouped them by ids and page batches: 50 ids for each Wikidata query and 20 pages for each request of introductions.

We also had to respect the rules of Wikimedia and to be sure that a failure would not make us restart everything:
- Each request has a `User-Agent` with a contact address, as asked by Wikimedia.
- We wait 0.3 s between two requests.
- After a timeout, a connection error or an error 429/500/502/503/504, we retry up to 4 times and wait longer each time (2, 4, 8... seconds). For the other errors (404, 400), retrying would not help, so we stop.
- Each batch is written in a raw JSONL file (`films.jsonl`, `people.jsonl`) to keep the already filled ids. When we run the script again, those ids will be skipped but a movie or a person with a failed request is not written, so it will be tried again next time.

### Results

| Films | Count | Share |
|------|------|---------|
| Records | 7,185 | 100 % |
| With synopsis | 5,925 | 82.5 % |
| No Wikidata item for this IMDb id | 489 | 6.8 % |
| No Plot section in the Wikipedia article | 443 | 6.2 % |
| No English Wikipedia article | 328 | 4.6 % |

| Distinct Principals | Count | Share |
|------|------|---------|
| Records | 11,277 | 100 % |
| With biography | 8,029 | 71.2 % |
| No Wikidata item for this IMDb id | 2,086 | 18.5 % |
| No English Wikipedia article (Wikidata facts only) | 1,161 | 10.3 % |
| Empty Wikipedia introduction | 1 | 0.0 % |

Here is the coverage of our enrichment. In each table, the lines with a missing value and the line with a text add up to the total number of records, so every record is either enriched or explained.
The coverage is lower for the people than for the movies. We could explain that by the fact that a lot of crew members may not have a Wikidata item or an English article.

### Provenance and missing information

For each record, we keep where the information comes from:
- `source_url`: the permanent link to the exact Wikipedia revision of the text (`?oldid=...`). If the article is modified later, our source stays the same.
- `wikidata_url`: the Wikidata page, which is the source of the facts about the people (dates, place, occupations).
- `wikidata_id` and `wikipedia_title`: to find the entity again.
- `retrieved_at`: the date and time (UTC) of the collection.

When we found nothing, the record still has a source. We use the Wikidata page if the item exists, and the Wikidata SPARQL endpoint if nothing was found at all. The cleaning step applies the same rule, so no record has a null `source_url`.

We decided to never invent or replace missing information with a default text. The value stays null and the `note` column explains why:

| Note | Meaning |
|------|------|
| No Wikidata item for this IMDb id | The id is not in Wikidata |
| No English Wikipedia article | The item exists but has no English article |
| No Plot section in the Wikipedia article | The article has no accepted synopsis section |
| Empty Wikipedia introduction | The article exists but the introduction is empty |
| No synopsis / biography available | Fallback added during the cleaning when the text and the note are both null |

These notes are also used to build the coverage tables above.

### Cleaning and schema enforcement (Spark)

The raw JSONL files are read again with Spark. Then we did the following:
1. **Explicit schemas:** we defined `FILM_SCHEMA` and `PEOPLE_SCHEMA` so that Spark does not guess the types. It is more reliable and it avoids an extra pass on the data.
2. **Text cleaning:** we trim the texts, we replace the series of blank lines by only one, and the empty strings become null.
3. **Dates:** `birth_date` and `death_date` are converted with `try_to_date`, and `retrieved_at` with `try_to_timestamp`. An invalid value becomes null instead of stopping the job.
4. **Duplicates:** we drop the duplicates on `tconst` and `nconst`, in case a record was written twice after an interruption.
5. **Joins:** we join the enriched movies with our selected movies on `tconst`, and the enriched people with our selected principals on `nconst`. We used left joins, so a selected movie or principal is never lost when the enrichment found nothing.
6. **Outputs:** the two results are saved in Parquet (`top_films` and `top_films_principals`) and the coverage report is written in `reports/03_enrichment.txt`.

The `top_films` table has one row for each pair (movie, genre), so the synopsis of a movie in several genres appears on each of its rows. But we only collected it once.

### Scalability

The Spark part scales with the data, because the selection (`explode`, window and joins) and the cleaning are distributed operations. We cache the selected datasets because we use them several times. We also use `toLocalIterator()` to send a Spark partition at a time instead of loading all the data in the memory of the driver. The ids are made distinct before, so a movie in several genres or a person in several movies is only requested once.

The slow part is the collection, because it is sequential and limited by the rate limit. The run time of the script depends on the size of our selection, not on the size of the IMDb tables. The resume system also makes the long collection safe: if it stops, we lose one batch at most.

### Methodological explanation

#### Methodology and why it was chosen

Our pipeline has three steps: data selection of movies and principals in Spark, data collection in Python from Wikipedia and Wikidata, and data cleaning in Spark. 
In the first step, we filtered our different datasets and made some inner joins as explained in the [Scope](#scope) section. Then we defined our sources as Wikipedia and Wikidata and applied their rules to get the data needed (biographies and synopses) as explained in the [Sources and collection](#sources-and-collection) section. We saved the raw enriched data in `data/enriched/raw/films.jsonl` and `data/enriched/raw/people.jsonl`. After saving the raw enriched data, we cleaned it with Spark by enforcing the types and saving it in Parquet. 

To search the movies and people in Wikipedia, we used their ids to prevent wrong matches (like people or movies with the same name). We also decided to keep the raw files separated from the cleaned data so that we never have to redo the slow collection when we change a cleaning rule. 

#### Alternatives considered and trade-offs

At first, we considered the source of our data research (text generation was prohibited). The subject suggested Wikipedia or other APIs. We chose Wikipedia because it is human-written and its content is open for API reading whereas IMDb forbids scraping.
Instead of searching directly on Wikipedia, we chose to use Wikidata to prevent unnoticeable mistakes like homonyms and remakes. That choice led to some data loss (around 7% of loss for the movies and 18% for the people). We still chose that method because the data loss was low compared to the error risk of not taking the right synopses or biography.
We chose to store the raw and final results in different formats because of their use. We need to write line by line in the raw file which is why we use a jsonl file.

Our goal was to have reliable data even with lower coverage and that our code scalability. 

## Streaming

## Open question
Full output: [reports/05_open_question.txt](reports/05_open_question.txt)

For the open question, we tried to answer the following question: **which film genres travel best internationally once popularity is taken into account, and has this changed between 1980 and 2025?**

We chose this question because the table `title.akas` lists the titles of each film in different regions, so we can use it to measure how far a film travels. But popular films have many more regional titles than the others. A simple ranking of the genres would only tell us which genres are popular, and this is why we wanted to take popularity into account.

### Answer

According to our dataset, **three genres travel better than the others: Animation, Western and News**. Their films have between 15 % and 19 % more regional titles than the films with the same popularity released in the same decade. With our main index, Animation is first, but this order is not robust: in our two robustness tests, Animation falls to third place behind Western or News. What is robust is the top three itself, and the bottom of the ranking: **Comedy, Musical, Documentary** and above all **Adult** travel less than expected.

The ranking changed between 1980 and 2025. **Animation** improved the most and has the highest value of the 2020s, whereas **Action, Thriller and Sport**, which had the highest indices in the 1980s and 1990s, are now close to the average. Finally, travelling far does not mean being better rated: the international reach is not linked to the average rating of a film.

### Scope and measure

We used the same definition of a film as in the enrichment step: `titleType = "movie"`, at least 40 minutes, between 1980 and 2025, with a rating and with at least one genre. This gives us **237,143 films**, and only 352 of them (0.1 %) have no regional title. We removed the region `XWW` because "World-wide" is not a country.

```
FIRST_YEAR, LAST_YEAR = 1980, 2025
MIN_FEATURE_RUNTIME = 40      # feature-length film, in minutes
MIN_GENRE_FILMS = 500         # genres with fewer films are left out
MIN_CELL_FILMS = 100          # minimum films for a (genre, decade) value
```

**Additional data:** we did not need any additional data. We only used the cleaned IMDb tables (`title.basics`, `title.ratings` and `title.akas`) from the preparation step, and no external source. We did not use the Wikipedia and Wikidata data from the enrichment step either, because the question only needs the regions, the votes, the ratings and the genres. The only limit is that the number of regional titles is a proxy of the real international distribution, and data like box office or release dates by country would have been better, but they are not in the dataset.

We defined the **international reach** of a film as the number of distinct regions where it has a title in `title.akas`. It is not a perfect measure of the real distribution, but it is the only one available in the dataset.

Our first result showed us that we could not use it directly: the correlation between the number of regions and the logarithm of the number of votes is **0.77**. A film with more than 100,000 votes has 47 regions on average, against 3 for a film with fewer than 10 votes. So we compare each film with the films which have the same popularity and the same decade:
- The **vote bucket** is `floor(log10(numVotes))`, capped at 5 (1-9 votes, 10-99 votes, ..., 100,000+ votes).
- The **export index** of a film is its number of regions divided by the mean number of regions of the films of the same bucket and decade. 1.00 means "as expected" and 1.20 means 20 % more regions than expected.

The index of a genre is the mean of the index of its films (a film with several genres is counted in each of them). We only kept the genres with at least 500 films.

### Which genres travel the best?

| Adjusted rank | Genre | Films | Avg regions | Raw rank | Export index |
|---:|------|---------:|---------:|---------:|---------:|
| 1 | Animation | 5,595 | 13.6 | 2 | 1.19 |
| 2 | Western | 787 | 11.2 | 9 | 1.16 |
| 3 | News | 669 | 6.6 | 21 | 1.15 |
| 4 | Adventure | 12,577 | 14.5 | 1 | 1.12 |
| 5 | Sci-Fi | 6,563 | 12.8 | 3 | 1.11 |
| 5 | Thriller | 24,475 | 11.5 | 7 | 1.11 |
| 7 | Action | 25,339 | 12.2 | 6 | 1.09 |
| 8 | Horror | 22,135 | 10.7 | 10 | 1.07 |
| 9 | Mystery | 10,232 | 12.7 | 4 | 1.06 |
| 9 | Family | 9,598 | 9.5 | 14 | 1.06 |
| 11 | Crime | 19,211 | 12.3 | 5 | 1.05 |
| 12 | Fantasy | 8,086 | 11.5 | 7 | 1.02 |
| 12 | Drama | 104,644 | 9.5 | 14 | 1.02 |
| 14 | Romance | 23,732 | 10.5 | 11 | 1.01 |
| 14 | War | 3,411 | 9.5 | 14 | 1.01 |
| 14 | Sport | 3,621 | 9.5 | 14 | 1.01 |
| 17 | Biography | 9,359 | 10.2 | 12 | 1.00 |
| 18 | History | 7,307 | 9.2 | 18 | 0.99 |
| 18 | Music | 7,092 | 8.5 | 19 | 0.99 |
| 20 | Documentary | 51,020 | 5.7 | 22 | 0.96 |
| 21 | Comedy | 56,585 | 9.6 | 13 | 0.94 |
| 22 | Musical | 2,740 | 7.5 | 20 | 0.93 |
| 23 | Adult | 2,236 | 2.4 | 23 | 0.57 |

Here is the ranking of the genres. The "raw rank" is the ranking with the mean number of regions, and the adjusted rank is the one with our export index. The adjustment changes the result a lot:
- **Adventure** is first with the raw mean (14.5 regions) but only fourth after the adjustment. A part of its raw result comes from its popularity.
- **News** (median of 30 votes) goes from the 21st to the 3rd place and **Western** from the 9th to the 2nd place. Their films are not very popular, but they have many regions compared to the films with the same popularity.
- The top three are close to each other (1.15 to 1.19). Apart from them and Adult, all the genres are between 0.93 and 1.12. **Adult (0.57) is the only real outlier.**

We also checked that this ranking does not come from a particularity of IMDb. IMDb adds machine-translated titles in 7 regions (`IN`, `DE`, `JP`, `FR`, `ES`, `IT`, `PT`) as a block, mostly on popular films: 96.5 % of the films with more than 100,000 votes have the full block. We calculated the index again in two other ways: without counting these 7 regions, and without the 10,769 films which have the full block.

| Genre | All regions | Without the 7 regions | Without the films with the block |
|------|---------:|---------:|---------:|
| Animation | 1.19 (1st) | 1.16 (3rd) | 1.15 (3rd) |
| Western | 1.16 (2nd) | 1.17 (2nd) | 1.17 (1st) |
| News | 1.15 (3rd) | 1.18 (1st) | 1.16 (2nd) |
| Adventure | 1.12 | 1.12 | 1.11 |
| Thriller | 1.11 | 1.11 | 1.13 |
| Sci-Fi | 1.11 | 1.10 | 1.13 |
| Action | 1.09 | 1.09 | 1.10 |
| Documentary | 0.96 | 0.95 | 0.96 |
| Comedy | 0.94 | 0.95 | 0.94 |
| Musical | 0.93 | 0.93 | 0.93 |
| Adult | 0.57 | 0.48 | 0.57 |

Here is the result of the robustness test. The three genres of the top stay at the top in every version, with an index between 1.15 and 1.19, and the bottom (Adult, Musical, Comedy, Documentary) does not move. The other genres change by 0.03 at most. But the order inside the top three changes: Animation is first only when we count all the regions, so a part of its advantage comes from the block. Western and News are also small genres (787 and 669 films), so we cannot say that one of the three travels better than the two others. We can only say that these three travel better than the rest.

### Has it changed between 1980 and 2025?

| Genre | 1980s | 1990s | 2000s | 2010s | 2020s |
|------|------:|------:|------:|------:|------:|
| Animation | 1.12 | 1.03 | 1.16 | 1.19 | 1.25 |
| Thriller | 1.20 | 1.23 | 1.15 | 1.08 | 1.09 |
| Action | 1.24 | 1.09 | 1.07 | 1.06 | 1.04 |
| Sport | 1.19 | 1.16 | 1.00 | 0.99 | 0.99 |
| Documentary | 1.12 | 0.96 | 0.96 | 0.95 | 0.95 |
| Biography | 1.14 | 0.98 | 0.98 | 1.01 | 0.96 |
| War | 1.12 | 0.90 | 1.01 | 0.98 | 1.01 |
| History | 0.96 | 0.87 | 0.98 | 1.00 | 1.00 |
| Comedy | 0.88 | 0.90 | 0.95 | 0.97 | 0.94 |
| Musical | 0.84 | 0.76 | 0.93 | 0.99 | 1.00 |
| Adventure | 1.13 | 1.11 | 1.11 | 1.12 | 1.13 |
| Drama | 1.03 | 1.01 | 1.00 | 1.03 | 1.02 |

Here are the genres where we can see a change, plus Adventure and Drama as examples of stable genres. We did not put the genres whose index changes by less than 0.1 over the five decades (Sci-Fi, Horror, Romance, Family, Mystery, Crime, Fantasy, Music). We also did not give a value when a genre has fewer than 100 films in a decade, so we cannot say much about Western, News and Adult, which have values for only three decades, one decade and two decades.

We can see these movements:
- **Animation** is the genre which improved the most (from 1.03 in the 1990s to 1.25 in the 2020s). It has the highest value of the 2020s, but Western is close (1.22).
- In the 1980s, **Action (1.24), Thriller (1.20) and Sport (1.19)** had the highest indices, and in the 1990s it was Thriller (1.23) and Sport (1.16). Today, Sport (0.99) and **Documentary (0.95)** are at or below the average, and Action (1.04) and Thriller (1.09) are only slightly above it.
- **Biography** goes down (1.14 to 0.96), **Musical** and **History** improve (0.76 to 1.00 and 0.87 to 1.00), and **Comedy** improves slowly (0.88 to 0.94). **War** goes down in the 1990s and then comes back to 1.01.

We must remember that the index is compared with the films of the same decade: a decrease means that a genre travels less than the other films of its decade, not that it has fewer regions than before.

### International reach and rating

We also wanted to know if the films which travel the most have better ratings. The correlation between the number of regions and the average rating is -0.04 (-0.02 with the export index). We also split the films into five groups with the quintiles of the export index (the groups have between 39,461 and 55,796 films because of tied values). The average rating is between 5.95 and 6.32 in each group, with no clear trend. So the international reach depends on the popularity of a film but not on its rating. We can only say this about the rating given by the IMDb voters, which is not a measure of the quality of a film.

### Sanity check

As a last check, we listed the three most exported films of the top 3 genres: Yobi, the Five Tailed Fox, Shaun the Sheep Movie and A Christmas Carol for Animation, The Harder They Fall, Unforgiven and Django Unchained for Western, and Food, Inc. for News. Most of them are well-known international titles, so the results look coherent. The exception is Yobi, which has only 1,075 votes but 109 regions (index of 7.39). It shows that a few films can have a very high index, and that the number of regions is not only linked to success. We also noticed that the News films look more like documentaries, so we stay careful with this genre.

### Methodological explanation

#### Methodology and why it was chosen

We did the whole analysis in Spark, with the cleaned tables of the preparation step. We think that this analysis is not trivial, because the answer cannot be read in a single table and a simple average would give a wrong result. It combines three tables and several Spark operations:
- **Three tables:** `title.basics` gives the type, the runtime, the year and the genres of the films, `title.ratings` gives the votes and the rating, and `title.akas` gives the regional titles. We joined `basics` and `ratings` with an inner join to keep the rated films. Then we kept only these films in `akas` with a `left_semi` join before any aggregation. This table has around 60 million rows, so we wanted to reduce it as soon as possible. After the count, we joined the result back to our films with a left join, and the films without regional title got 0.
- **Genre explosion:** the `genres` column is an array, so we used `explode` to get one row for each pair (film, genre). A film with several genres is counted in each of them.
- **Conditional aggregation:** in only one pass over `akas`, we calculated three counts for each film with `countDistinct` and `when`: all the regions, the regions without the 7 regions of the machine-translated block, and the number of regions of the block. We used them for the robustness test without reading the big table three times.
- **Normalisation by group:** we calculated the mean number of regions of each group (vote bucket and decade), joined it back to the films and divided the number of regions of each film by it. This gives the export index. We also used window functions to rank the genres, a pivot to build the table by decade, and `approxQuantile` for the quintiles.

We needed this normalisation because of our first result: the number of regions is strongly linked to the number of votes (correlation of 0.77). Without it, we would only have ranked the most popular genres. We used the number of regions because it is simple and available for all the films, and a logarithm for the votes because the number of votes goes from 1 to millions and the regions increase with the order of magnitude of the votes. We compared the films by decade because the number of regional titles is not the same for an old and a recent film. We also limited the results to the genres with 500 films and the cells with 100 films, so that a few films cannot make a ranking alone.

We built the answer in 6 steps: the link with popularity, the ranking, the robustness test, the evolution by decade, the link with the rating and the sanity check. Each step relies on the previous one: we needed to show that popularity matters before adjusting for it, and we needed to check how solid the ranking is before looking at its evolution.

We stayed consistent with the choices of the preparation step: the titles with no region were removed from the count, so the number of regions can be underestimated for some films.

#### Alternatives considered and trade-offs

The first idea was to rank the genres with their average number of regions. But our first results showed that popularity explained most of the differences, so this ranking would have been a ranking of the most popular genres (Adventure first, News 21st). This is why we used the export index. The limit is that the buckets are wide (a factor of 10), so some effect of popularity can stay inside a bucket. We accepted it because the index is easy to read (1.20 means 20 % more regions than expected).

For the machine-translated block, we could have removed it directly, but we did not know how much it changed the results. So we kept the full index as the main result and compared it with two other versions. The test showed that the general picture is stable (same top three, same bottom) but that the order inside the top three is not. This is why we keep the simplest version as the main result, but we only conclude on the group of three genres and not on a single winner.

We used the regions and not the languages, because the `language` column of `title.akas` has more missing values than the `region` column (32.55 % against 21.53 %, see the Preparation section).

Our analysis also has some limits:
- A regional title is not a proof that the film was distributed in this country.
- A film with several genres is counted in each of its genres, so the genres are not independent.
- The results for the small genres (Western, News, Adult) and for the first decades rely on fewer films.
- We can say which genres have more regional titles than the other films with the same popularity, but we cannot say why.

Our goal was to answer the question with a measure which is simple to explain and checked by a robustness test, even if it is only a proxy of the real international distribution.