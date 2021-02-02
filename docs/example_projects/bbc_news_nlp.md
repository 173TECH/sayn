# BBC News NLP

This is a sample SAYN project. It shows you how to implement and use SAYN for data modelling and processing.

In this project, we will demonstrate how to use SAYN alongside other Python packages to scrape and perform basic NLP on the BBC News RSS feeds.

In addition to SAYN, this project uses the following packages:

* `feedparser`: used for getting data from rss feeds
* `numpy, pandas, nltk`: used for data processing
* `matplotlib, wordcloud, pillow`: used for visualisations


## Step 1 Extract Group

First, we need to define our `extract` group in our tasks folder. This group will only include the `load_data` task. This is quite a simple python task which will use the `LoadData` class from `load_data.py` which we will create later. Our `load_data` task will have two parameters:

* `table`: name of the table we plan to create in our database
* `links`: list of links to rss feeds

??? example "tasks/extract.yaml"
    ```yaml
    tasks:
      load_data:
        type: python
        class: load_data.LoadData
        parameters:
          table: logs_bbc_feeds
          links:
            - http://feeds.bbci.co.uk/news/england/rss.xml
            - http://feeds.bbci.co.uk/news/wales/rss.xml
            - http://feeds.bbci.co.uk/news/scotland/rss.xml
            - http://feeds.bbci.co.uk/news/northern_ireland/rss.xml
            - http://feeds.bbci.co.uk/news/world/us_and_canada/rss.xml
            - http://feeds.bbci.co.uk/news/world/middle_east/rss.xml
            - http://feeds.bbci.co.uk/news/world/latin_america/rss.xml
            - http://feeds.bbci.co.uk/news/world/europe/rss.xml
            - http://feeds.bbci.co.uk/news/world/asia/rss.xml
            - http://feeds.bbci.co.uk/news/world/africa/rss.xml

    ```

Next, we need to create a function for fetching our data from the BBC RSS feeds. We will import this into our `load_data` task to keep our code cleaner.

The `fetch_data` function uses the `feedparser.parse` method to fetch the raw data from the rss feed link. It then converts the data into a `pandas data frame` to make it easier to work with. Notably, the function also drops some incompatible columns with our SQLite database, these are the following:

* title_detail
* summary_detail
* links
* published_parsed

These columns are just JSON object representations of information we already have, so there is no need to keep these columns.

The function also extracts the source of each article and adds it under the `source` column. Lastly, the function assigns a unique_id to each article which is based on its article id and the source it originates from. This is because the same article may be published in multiple sources with the same id, which means our original ids are not unique and could be misleading.

??? example "python/misc/data_fetch.py"
    ```python
    import feedparser as f
    import pandas as pd

    def fetch_data(link):
        raw_data = f.parse(link)
        data = pd.DataFrame(raw_data.entries)
        data.drop(["title_detail", "summary_detail", "links", "published_parsed"], axis=1, inplace=True)
        data["source"] = link[29:-8].replace("/","_")
        data["unique_id"] = data["id"] + data["source"]
        return data

    ```

Finally, we will create our `load_data` class.

Our LoadData inherits properties from SAYN's PythonTask, in addition it will have two methods:

* `setup`: sets the order of steps to run
* `run`: defines what each step does during the run

???+ attention
     Please note that all methods (`setup` and `run `) need to return either `self.success()` or `self.fail()` in order to run.


This run has the following steps:

* `Assigning required parameters`: assigns parameters to be used in the run, updates table parameter to include user_prefix if one is specified
* `Appending data to dataframe`: loops through the links array, appends data from each link to a dataframe
* `Updating database`: loads dataframe into SQLite database using `pandas.to_sql` method

??? example "python/load_data.py"
    ```python
    import pandas as pd
    from sayn import PythonTask
    from .misc.data_fetch import fetch_data


    class LoadData(PythonTask):
        def setup(self):
            self.set_run_steps(
                [
                    "Assigning required parameters",
                    "Appending data to dataframe",
                    "Loading filled dataframe to database"
                ]
            )
            return self.success()


        def run(self):

            with self.step("Assigning required parameters"):
                links = self.parameters["links"]
                table = self.parameters["user_prefix"] + self.task_parameters["table"]


            with self.step("Appending data to dataframe"):

                df = pd.DataFrame()

                for link in links:

                    temp_df = fetch_data(link)
                    n_rows = len(temp_df)
                    df = df.append(temp_df)
                    self.info(
                        f"Loading {n_rows} rows into destination: {table}...."
                    )


            with self.step("Loading filled dataframe to database"):
                if df is not None:

                    df.to_sql( table
                               ,self.default_db.engine
                               ,if_exists="append"
                               ,index=False)

                    self.info("Load done.")


            return self.success()

    ```



## Step 2 Modelling Group

Currently our `load_data` task appends data to our database but it does not filter out any potential duplicates that we might encounter after multiple runs. This is where the `modelling` group comes in, we can define an AutoSQL task to filter out any duplicates.

First, we need to create a sql query in our `sql` folder that will filter out any duplicates; we will call it `f_bbc_feeds.sql`

*** Note: {{user_prefix}} is set dynamically based on what you set it to in settings.yaml ***

??? example "sql/dim_bbc_feeds.sql"
    ```sql
    SELECT DISTINCT unique_id
         , id
         , title
         , summary
         , link
         , guidislink
         , published
         , source
    FROM {{user_prefix}}logs_bbc_feeds
    ```

Next, we will define a modelling preset in `project.yaml`. Adding this preset will simplify the code in `modelling.yaml` while also allowing us to set dynamic file and table names.

*** Note: {{ task.name }} returns the name of task the preset is assigned to  ***

??? example "project.yaml"
    ```yaml
    required_credentials:
      - warehouse

    default_db: warehouse

    presets:

        modelling:
          type: autosql
          materialisation: table
          file_name: "{{ task.name }}.sql"
          destination:
            table: "{{ user_prefix }}{{ task.name }}"

    parameters:
      user_prefix:

    ```

Now that we have the modelling preset, we can use it in the `modelling` group. Since we want `dim_bbc_feeds` to run after our `load_data` task, we will need to set the parents of the task to `load_data`.

??? example "tasks/modelling.yaml"
    ```yaml
    tasks:
        dim_bbc_feeds:
          preset: modelling
          parents:
            - load_data

    ```

## Step 3 Data Science Group

Now that we have our cleaned dataset, we can utilise python tasks to do some natural language processing on our text data. In particular, we will use two libraries for this analysis:

* `nltk`: for basic text statistics
* `wordcloud`: for generating wordcloud visualisations

To keep the code clean, we will create a few more helper functions in `processing.py`:

* `desc_text`: provides counts of letters, words and sentences in an article
* `word_cloud`: generates a wordcloud visualisation
* `words`: returns stopwords from the wordcloud package

??? example "python/misc/processing.py"
    ```python
    import numpy as np
    import matplotlib.pyplot as plt
    from PIL import Image
    from wordcloud import WordCloud, STOPWORDS
    from nltk.tokenize import word_tokenize, sent_tokenize


    def desc_text(df, text_field, language):
        df[text_field + "_letters"] = df[text_field].fillna("").str.len()
        df[text_field + "_words"] = df[text_field].fillna("").apply(lambda x: len(word_tokenize(x, language=language)))
        df[text_field +"_sentences"] = df[text_field].fillna("").apply(lambda x: len(sent_tokenize(x, language=language)))


    def word_cloud(name, text, stopwords, b_colour = "white", c_colour = "firebrick", show=False):
        try:
            mask = np.array(Image.open(f"python/img/masks/{name}_mask.png"))
        except:
            mask = None

        wordcloud = WordCloud(stopwords=stopwords
                              , max_words=100
                              , mask=mask
                              , background_color = b_colour
                              , contour_width=1
                              , contour_color= c_colour).generate(text)

        wordcloud.to_file(f"python/img/{name}_wordcloud.png")

        if show:
            plt.imshow(wordcloud, interpolation="bilinear")
            plt.axis("off")
            plt.show()


    def words():
        return set(STOPWORDS)

    ```
???+ attention
     If you are experiencing issues with this code, you may need to install punkt. You can do it by running the following:
     ```python
     import nltk
     nltk.download('punkt')
     ```

First, we need to create the `data_science` group in the `tasks` folder. There will be two tasks within this group:

* `nlp`: generates the text statistics
* `wordcloud`: generates the wordclouds

Since both of these tasks are children of the `dim_bbc_feeds` task, we will need to set the parents and table parameters for both tasks to `dim_bbc_feeds`.

The `nlp` task has a `text` parameter, this parameter specifies which columns have text for processing.
The `wordcloud` task has a `stopwords` parameter, this parameter provides additional context related stopwords (e.g. "say" and its variations seem to be very common in summaries, however they are not very informative).


??? example "tasks/data_science.yaml"
    ```yaml
    tasks:      
      nlp:
        type: python
        class: nlp.LanguageProcessing
        parents:
          - dim_bbc_feeds
        parameters:
          table: dim_bbc_feeds
          text:
            - title
            - summary

      wordcloud:
        type: python
        class: wordcloud.RenderCloud
        parents:
          - dim_bbc_feeds
        parameters:
          table: dim_bbc_feeds
          stopwords:
            - say
            - said
            - says
            - will
            - country
            - US
            - England
            - Scotland
            - Wales
            - NI
            - Ireland
            - Europe
            - BBC
            - yn

    ```


Next, we can define the class for the `wordcloud` task. This run has the following steps:

* `Assigning required parameters`: similar to LoadData step
* `Aggregating texts`: aggregates article summaries and groups them by source
* `Getting stopwords`: updates the wordcloud stopwords with ones specified in the parameters
* `Generating clouds`: generates a wordcloud for each source, as well as the full dataset

??? example "python/wordcloud.py"
    ```python
    import pandas as pd
    from .misc.processing import words, word_cloud
    from sayn import PythonTask


    class RenderCloud(PythonTask):
        def setup(self):
            self.set_run_steps(
                [
                    "Assigning required parameters",
                    "Aggregating texts",
                    "Getting stopwords",
                    "Generating clouds"
                ]
            )
            return self.success()


        def run(self):

            with self.step("Assigning required parameters"):
                table = self.parameters["user_prefix"] + self.task_parameters["table"]


            with self.step("Aggregating texts"):

                df = pd.DataFrame(self.default_db.read_data(f"SELECT * FROM {table}"))
                full_text = " ".join(article for article in df.summary)

                sources = df.groupby("source")
                grouped_texts = sources.summary.sum()


            with self.step("Getting stopwords"):

                stopwords = words()
                stopwords.update(self.parameters["stopwords"])


            with self.step("Generating clouds"):

                self.info("Generating bbc_wordcloud.png")
                word_cloud("bbc", full_text, stopwords, b_colour = "white", c_colour = "black")
                self.info("bbc_wordcloud.png generated succesfully!")

                # Source specific wordclouds

                for group, text in zip(grouped_texts.keys(), grouped_texts):
                    self.info(f"Generating {group}_wordcloud.png")
                    word_cloud(group, text, stopwords)
                    self.info(f"{group}_wordcloud.png generated succesfully!")


            return self.success()

    ```

Moving on, we can define the class for the `nlp` task. This run has the following steps:

* `Assigning required parameters`: similar to LoadData step
* `Processing texts`: loops through text_fields, generates text statistics on each entry
* `Updating database`: similar to LoadData step, has additional debugging information

??? example "python/nlp.py"
    ```python
    import pandas as pd
    from .misc.processing import desc_text
    from sayn import PythonTask


    class LanguageProcessing(PythonTask):
        def setup(self):
            self.set_run_steps(
                [
                    "Assigning required parameters",
                    "Processing texts",
                    "Updating database"
                ]
            )
            return self.success()

        def run(self):

            with self.step("Assigning required parameters"):
                table = self.parameters["user_prefix"] + self.task_parameters["table"]
                text_fields = self.parameters["text"]


            with self.step("Processing texts"):

                df = pd.DataFrame(self.default_db.read_data(f"SELECT * FROM {table}"))

                for t in text_fields:
                    self.info(f"Processing texts for {t} field")
                    desc_text(df, t, "english")
                    self.info("Processing Completed!")


            with self.step("Updating database"):
                if df is not None:
                    output = f"{table}_{self.name}"
                    n_rows = len(df)
                    self.info(f"Loading {n_rows} rows into destination: {output}....")
                    df.to_sql( output,
                               self.default_db.engine,
                               if_exists="replace",
                               index=False)
                    self.info("Load done.")


            return self.success()

    ```
