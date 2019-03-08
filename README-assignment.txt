

                               ~= Yousician =~

                        Data Engineer - Small Exercise

                                    ~~~~~
                                   28/06/18

Hello there! Imagine you are working for an amazing company named "Ovecell"
that has published two different games: "Wild Wild Chords" and "HarmonicaBots".

Lately, the marketing department has expressed the need for a new game
with revolutionary gameplay to attract more users. For such endeavor, the
Ovecell data analysts will first need to analyze existing data from previous
games to see what makes the users stick. At the moment, the data is just chilling
out in the production database clusters.

The Ovecell devops team could give access to the databases to the analysts,
but the analysts hate non-uniform data with a passion and will not touch it.
Somebody has to merge all the tables of each database and store the results in
a single data warehouse. Luckily, the only data that is actually relevant for
researching revolutionary gameplay is the user account data.

This is where Ovecell will need your expertise! The devops team has already
created a scheduled script to dump the data including the accounts of the 
users from the previously mentioned two games and you should have been provided 
a copy of the account data extracted at a specific date.

(See the data/wwc/2018/06/28/*.json and data/hb/2018/06/28/*.csv files)

Could you develop a tiny pipeline for Ovecell's ETL?

Sadly, some of the data is not clean, since the previously mentioned games,
and their database schemas, were refactored many times during the past years.
The team lead thinks you "just need to deal with it".

In short, the idea would be to have a data warehouse based on SQLite. Create
a database schema and import the user accounts before the marketing
department loses their focus.

Some of the data is missing, which is not a problem in general, but try to
get the locations of as many of the users as possible, because the data
analysts will mainly try A/B testing based on countries.


                                 REQUIREMENTS
                                 ============

- Use Python.
- Write a script which take as an argument a game name ('wwc' or 'hb') 
  and the date in format of 'YYYY-MM-DD'.
- Write tests.
- Follow the KISS principle. So keep it simple, but we do expect well
  designed code.
- Provide the script and input data in a single .zip package.
- Provide all instructions to do the setup, the easier it is for us to
  get it running the better.


                                   QUESTIONS 
                                   =========

To test your new dataset before giving it to the analysts, try a few exercises:

1) Could you find what is the gender ratio in each game?
2) Try to list the youngest and oldest player per country.
3) If you suddenly had millions of new accounts to process per day, how would
you make the whole ETL faster?


If you have any questions about the requirements, please don't hesitate to ask clarifications!

Good luck!
