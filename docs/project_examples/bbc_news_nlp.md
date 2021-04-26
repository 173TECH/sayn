# SAYN Project Example: BBC News NLP

## Project Description

#### Overview

This is an example SAYN project which shows how to use SAYN for data modelling and processing. You can find the GitHub repository
[here](https://github.com/173TECH/rss_tutorial){target="\_blank"}.

This project does the following:

* Extracts article data from BBC RSS feeds
* Loads it into a SQLite database
* Cleans the extracted data
* Performs some basic text analysis on the transformed data

#### Features Used

* [Python tasks](../tasks/python.md) to extract and analyse data
* [Autosql tasks](../tasks/autosql.md) to automate SQL transformations.
* Usage of [parameters](../parameters.md) to make the code dynamic.
* Usage of [presets](../presets.md) to define tasks.

In addition to SAYN, this project uses the following packages:

* `feedparser`: used for getting data from rss feeds
* `numpy, pandas, nltk`: used for data processing
* `matplotlib, wordcloud, pillow`: used for visualisations

#### Running The Project

* clone the repository with the command `git clone https://github.com/173TECH/rss_tutorial`.
* rename the `settings_sample.yaml` file to `settings.yaml`.
* install the project dependencies by running the `pip install -r requirements.txt` command from the root of the project folder.
* run all SAYN commands from the root of the project folder.

## Implementation Details


#### Step 1: Extract Task Group

Quick Summary:

* Create the task group `extract.yaml`
* Create a [python task](../tasks/python.md) to extract and load the data

First, we need to define our `extract` group in our tasks folder. This group will only include the `load_data` task. This is quite a simple [python task](../tasks/python.md) which will use the `LoadData` class from `load_data.py` which we will create later. Our `load_data` task will have two [parameters](../parameters.md):

* `table`: name of the table we plan to create in our database
* `links`: list of links to rss feeds

???+ note
     Parameters are not a requirement, however parameters make the code dynamic which is useful for reusability.

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
##### `LoadData` Class

Next, we will create our `LoadData` class.

Our LoadData inherits properties from SAYN's PythonTask, in addition it will have 3 methods:

* `fetch_bbc_data`: fetches data from the BBC RSS feeds
* `setup`: sets the order of steps to run
* `run`: defines what each step does during the run

???+ attention
     Please note that methods `setup` and `run ` need to return either `self.success()` or `self.fail()` in order to run.

###### Fetch BBC Data

The `fetch_bbc_data` function uses the `feedparser.parse` method to fetch the raw data from the rss feed link. It then converts the data into a `pandas dataframe` to make it easier to work with. Notably, the function also drops some incompatible columns with our SQLite database, these are the following:

* title_detail
* summary_detail
* links
* published_parsed

These columns are just JSON object representations of information we already have, so there is no need to keep these columns.

The function also extracts the source of each article and adds it under the `source` column. Lastly, the function assigns a unique_id to each article which is based on its article id and the source it originates from. This is because the same article may be published in multiple sources with the same id, which means our original ids are not unique and could be misleading.

###### `load_data` Task Details

This task has the following steps:

* `Appending BBC data to dataframe`: loops through the links array, appends data from each link to a dataframe
* `Updating database`: loads dataframe into SQLite database using `pandas.to_sql` method


!!! tip
    self.parameters["user_prefix"] is set dynamically based on what you set it to in project.yaml, this can also be overwritten in settings.yaml

??? example "python/load_data.py"
    ```python
    import pandas as pd
    import feedparser as f
    from sayn import PythonTask


    class LoadData(PythonTask):
        def fetch_bbc_data(self, link):
            """Parse and label RSS BBC News data then return it in a pandas DataFrame"""
            raw_data = f.parse(link)
            data = pd.DataFrame(raw_data.entries)
            data.drop(
                ["title_detail", "summary_detail", "links", "published_parsed"],
                axis=1,
                inplace=True,
            )
            data["source"] = link[29:-8].replace("/", "_")
            data["unique_id"] = data["id"] + data["source"]
            return data

        def setup(self):
            self.set_run_steps(["Appending BBC data to dataframe", "Updating database"])
            return self.success()

        def run(self):

            with self.step("Appending BBC data to dataframe"):

                links = self.parameters["links"]
                table = self.parameters["user_prefix"] + self.task_parameters["table"]

                df = pd.DataFrame()

                for link in links:

                    temp_df = self.fetch_bbc_data(link)
                    n_rows = len(temp_df)
                    df = df.append(temp_df)
                    self.info(f"Loading {n_rows} rows into destination: {table}....")

            with self.step("Updating database"):
                if df is not None:

                    df.to_sql(
                        table, self.default_db.engine, if_exists="append", index=False
                    )

            return self.success()
    ```


#### Step 2: Modelling Group

Quick Summary:

* Create the SQL query `dim_bbc_feeds.sql` to filter out duplicates
* Create a modelling [preset](../presets.md) in `project.yaml`
* Create the task group `modelling.yaml`


Currently our `load_data` task appends data to our database but it does not filter out any potential duplicates that we might encounter after multiple runs. This is where the `modelling` group comes in, we can define an [AutoSQL task](../tasks/autosql.md) to filter out any duplicates.

##### `dim_bbc_feeds` Task

First, we need to create a sql query in our `sql` folder that will filter out any duplicates; we will call it `dim_bbc_feeds.sql`

!!! tip
    {{user_prefix}} is set dynamically based on what you set it to in project.yaml, this can also be overwritten in settings.yaml

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

Next, we will define a modelling [preset](../presets.md) in `project.yaml`. [Presets](../presets.md) enable you to create a task prototype which can be reused when defining tasks. Hence, the modelling [preset](../presets.md) will simplify the code in `modelling.yaml` while also allowing us to set dynamic file and table names.

!!! tip
    {{ task.name }} returns the name of task the preset is assigned to

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

Now that we have the modelling [preset](../presets.md), we can use it in the `modelling` group. Since we want `dim_bbc_feeds` to run after our `load_data` task, we will need to set the parents of the task to `load_data`.

??? example "tasks/modelling.yaml"
    ```yaml
    tasks:
        dim_bbc_feeds:
          preset: modelling
          parents:
            - load_data

    ```


#### Step 3: Data Science Group

Quick Summary:

* Create the task group `data_science.yaml`
* Create the [python task](../tasks/python.md) `wordcloud` to generate wordclouds
* Create the [python task](../tasks/python.md) `nlp` to generate text statistics
* Create the [AutoSQL task](../tasks/autosql.md) `dim_bbc_feeds_nlp_stats` to calculate aggregate statistics grouped by source

Now that we have our cleaned dataset, we can utilise [python tasks](../tasks/python.md) to do some natural language processing on our text data. In particular, we will use two libraries for this analysis:

* `nltk`: for basic text statistics
* `wordcloud`: for generating wordcloud visualisations

First, we need to create the `data_science` group in the `tasks` folder. There will be two tasks within this group:

* `nlp`: generates the text statistics
* `wordcloud`: generates the wordclouds

Since both of these tasks are children of the `dim_bbc_feeds` task, we will need to set the parents and table [parameters](../parameters.md) for both tasks to `dim_bbc_feeds`.

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

##### `RenderCloud` Class

Next, we can define the class `RenderCloud` for the `wordcloud` task. `RenderCloud` has 3 methods:

* `word_cloud`: generates a wordcloud visualisation
* `setup`: sets the order of steps to run
* `run`: defines what each step does during the run

???+ attention
     Please note that methods `setup` and `run ` need to return either `self.success()` or `self.fail()` in order to run.

###### `wordcloud` Task Details

This task has the following steps:

* `Grouping texts`: aggregates article summaries and groups them by source (summaries are used instead of titles since they tend to be longer)
* `Generating clouds`: generates a wordcloud for each source, as well as the full dataset

??? example "python/wordcloud.py"
    ```python
    import pandas as pd
    import numpy as np
    import matplotlib.pyplot as plt
    from sayn import PythonTask
    from PIL import Image
    from wordcloud import WordCloud, STOPWORDS


    class RenderCloud(PythonTask):
        def word_cloud(
            self, name, text, stopwords, b_colour="white", c_colour="firebrick", show=False
        ):
            """Word cloud generating function"""

            # attempt to find a compatible mask

            try:
                mask = np.array(Image.open(f"python/img/masks/{name}_mask.png"))
            except:
                mask = None

            wordcloud = WordCloud(
                stopwords=stopwords,
                max_words=100,
                mask=mask,
                background_color=b_colour,
                contour_width=1,
                contour_color=c_colour,
            ).generate(text)

            # store wordcloud image in "python/img"

            wordcloud.to_file(f"python/img/{name}_wordcloud.png")

            # declare show=True if you want to show wordclouds

            if show:
                plt.imshow(wordcloud, interpolation="bilinear")
                plt.axis("off")
                plt.show()

        def setup(self):
            self.set_run_steps(["Grouping texts", "Generating clouds"])
            return self.success()

        def run(self):

            with self.step("Grouping texts"):

                table = self.parameters["user_prefix"] + self.task_parameters["table"]

                df = pd.DataFrame(self.default_db.read_data(f"SELECT * FROM {table}"))
                full_text = " ".join(article for article in df.summary)

                sources = df.groupby("source")
                grouped_texts = sources.summary.sum()

            with self.step("Generating clouds"):

                stopwords = STOPWORDS.update(self.parameters["stopwords"])
                self.info("Generating bbc_wordcloud.png")
                self.word_cloud(
                    "bbc", full_text, stopwords, b_colour="white", c_colour="black"
                )

                # Source specific wordclouds

                for group, text in zip(grouped_texts.keys(), grouped_texts):
                    self.info(f"Generating {group}_wordcloud.png")
                    self.word_cloud(group, text, stopwords)

            return self.success()

    ```

##### `LanguageProcessing` Class

Moving on, we can define the class `LanguageProcessing` for the `nlp` task. `LanguageProcessing` has 3 methods:

* `desc_text`: provides counts of letters, words and sentences in an article
* `setup`: sets the order of steps to run
* `run`: defines what each step does during the run

???+ attention
     Please note that methods `setup` and `run ` need to return either `self.success()` or `self.fail()` in order to run.

###### `nlp` Task Details

This task has the following steps:

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

##### `dim_bbc_feeds_nlp_stats` Task

Now that we have individual article statistics, it would be a good idea to create an additional modelling task to find some aggregate statistics grouped by source. Let's create another SQL query called `dim_bbc_feeds_nlp_stats` in the `sql` folder. This query will give us the grouped by source average of the text statistics generated by the `nlp` task.

??? example "sql/dim_bbc_feeds_nlp_stats.py"
    ```sql
    SELECT source
         , AVG(title_letters) AS average_tl
         , AVG(title_words) AS average_tw
         , AVG(title_sentences) AS average_ts
         , AVG(summary_letters) AS average_sl
         , AVG(summary_words) AS average_sw
         , AVG(summary_sentences) AS average_ss

    FROM {{user_prefix}}dim_bbc_feeds_nlp

    GROUP BY 1

    ORDER BY 1
    ```

Finally, we can add the `dim_bbc_feeds_nlp_stats` task to the the `modelling` group. Like the previous modelling task, we will create this task using the modelling [preset](../presets.md) in `project.yaml`; setting the parents parameter to `nlp`. We want to materialise this query as a view; therefore, we will need to overwrite the materialisation parameter of the preset.

??? example "modelling.yaml"
    ```yaml
    tasks:
        dim_bbc_feeds:
          preset: modelling
          parents:
            - load_data

        dim_bbc_feeds_nlp_stats:
          preset: modelling
          materialisation: view
          parents:
            - nlp
    ```


#### Step 4: Run the project

All that's left is to run the project in the command line. Change your directory to this project's folder and enter `sayn run`.

???+ attention
     Please note that if you did not clone the git repo, you may have some issues with the wordcloud generation. We recommend you create a folder called `img` within the `python` folder, if you do not already have one.
