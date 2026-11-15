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

## Streaming


