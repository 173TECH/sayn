# SAYN Project Example: BBC News NLP

## Project Description

#### Overview

This is an example SAYN project which shows how to use SAYN for data modelling and processing. You can find the GitHub repository
[here](https://github.com/173TECH/sayn_project_example_nlp_news_scraping){target="\_blank"}.

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

* RSS feed data extraction: `feedparser`
* Data processing: `numpy`, `pandas`, `nltk`
* Visualisations: `matplotlib`, `wordcloud`, `pillow`

#### Running The Project

* Clone the repository with the command `git clone https://github.com/173TECH/sayn_project_example_nlp_news_scraping`.
* Rename the `sample_settings.yaml` file to `settings.yaml`.
* Install the project dependencies by running the `pip install -r requirements.txt` command from the root of the project folder.
* Run all SAYN commands from the root of the project folder.
<br>
## Implementation Details

### Step 1: Extract Task Group

Quick Summary:

* Create the task group `extract.yaml`
* Create a [python task](../tasks/python.md) to extract and load the data

<br>

#### Task Details (`load_data`)

First, we need to define our `extract` group in our tasks folder. This group will only include the `load_data` task. This is quite a simple [python task](../tasks/python.md) which will use the `LoadData` class from `load_data.py` which we will create later.

Our `load_data` task will have two [parameters](../parameters.md):

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

???+ note
     Parameters are not a requirement, however parameters make the code dynamic which is useful for reusability.

The `load_data` task will have the following steps:

* `Appending BBC data to dataframe`: loops through the links array, appends data from each link to a dataframe
* `Updating database`: loads dataframe into SQLite database using `pandas.to_sql` method


###### LoadData Class

Next, we will create our `LoadData` class.

Our LoadData inherits properties from SAYN's PythonTask, in addition it will have 3 methods:

* `fetch_bbc_data`: fetches data from the BBC RSS feeds
* `setup`: sets the order of steps to run
* `run`: defines what each step does during the run

???+ attention
     `fetch_bbc_data` is a utility method for this task, while `setup` and `run` are the usual SAYN methods. Please note that methods `setup` and `run` need to return either `self.success()` or `self.fail()` in order to run.

###### Utility Method (`fetch_bbc_data`)

The `fetch_bbc_data` function uses the `feedparser.parse` method to fetch the raw data from the rss feed link. It then converts the data into a `pandas dataframe` to make it easier to work with.

The function also extracts the source of each article and adds it under the `source` column.

Lastly, the function assigns a `unique_id` to each article which is based on its article id and the source it originates from. This is because the same article may be published in multiple sources with the same id, which means our original ids are not unique and could be misleading.

??? example "python/load_data.py"
    ``` python
    import pandas as pd
    import feedparser as f
    from sayn import PythonTask

    class LoadData(PythonTask):

        def fetch_bbc_data(self, link):
            """Parse and label RSS BBC News data then return it in a pandas DataFrame"""

            # get data from supplied link
            raw_data = f.parse(link)

            # transform data to dataframe
            data = pd.DataFrame(raw_data.entries)

            # remove incompatible columns
            data.drop(
                ["title_detail", "summary_detail", "links", "published_parsed"],
                axis=1,
                inplace=True,
            )

            # get the source (this only works for BBC RSS feeds)
            data["source"] = link[29:-8].replace("/", "_")

            # generating ids to be unique, since same story ids can be published in different sources
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

??? tip
    `self.parameters["user_prefix"]` is set dynamically based on what you set it to in project.yaml, this can also be overwritten in settings.yaml

### Step 2: Modelling Group

Quick Summary:

* Create the SQL query `dim_bbc_feeds.sql` to filter out duplicates
* Create a modelling [preset](../presets.md) in `project.yaml`
* Create the task group `modelling.yaml`

<br>

#### Task Details (`dim_bbc_feeds`)

Currently our `load_data` task appends data to our database but it does not filter out any potential duplicates that we might encounter after multiple runs. This is where the `modelling` group comes in, we can define an [AutoSQL task](../tasks/autosql.md) to filter out any duplicates.

First, we need to create a sql query in our `sql` folder that will filter out any duplicates; we will call it `dim_bbc_feeds.sql`

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

??? tip
    `{{user_prefix}}` is set dynamically. The default value is set in `project.yaml`. This can be overwritten using profiles in `settings.yaml`.


Next, we will define a modelling [preset](../presets.md) in `project.yaml`. [Presets](../presets.md) enable you to create a task prototype which can be reused when defining tasks. Hence, the modelling [preset](../presets.md) will simplify the code in `modelling.yaml` while also allowing us to set dynamic file and table names.

???+ attention
     Presets defined in `project.yaml` are project level presets, you can also define presets within individual task groups.

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

??? tip
    `{{ task.name }}` returns the name of task

Now that we have the modelling [preset](../presets.md), we can use it in the `modelling` group. Since we want `dim_bbc_feeds` to run after our `load_data` task, we will need to set the parents of the task to `load_data`.

??? example "tasks/modelling.yaml"
    ```yaml
    tasks:
        dim_bbc_feeds:
          preset: modelling
          parents:
            - load_data

    ```

### Step 3: Data Science Group

Quick Summary:

* Create the task group `data_science.yaml`
* Create the [python task](../tasks/python.md) `wordcloud` to generate wordclouds
* Create the [python task](../tasks/python.md) `nlp` to generate text statistics
* Create the [AutoSQL task](../tasks/autosql.md) `dim_bbc_feeds_nlp_stats` to calculate aggregate statistics grouped by source

<br>

#### Group Overview

Now that we have our cleaned dataset, we can utilise [python tasks](../tasks/python.md) to do some natural language processing on our text data. In particular, we will use two libraries for this analysis:

* `nltk`: for basic text statistics
* `wordcloud`: for generating wordcloud visualisations

First, we need to create the `data_science` group in the `tasks` folder. There will be two tasks within this group:

* `nlp`: generates the text statistics
* `wordcloud`: generates the wordclouds

Both tasks will use data from our `dim_bbc_feeds` table, therefore we will need to set their their table [parameters](../parameters.md) to `dim_bbc_feeds`. Since both of these tasks are children of the `dim_bbc_feeds` task, we will also need to set their parents attributes to `dim_bbc_feeds`.

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


#### Task Details (`wordcloud`)

The `wordcloud` task will have the following steps:

* `Grouping texts`: aggregates article summaries and groups them by source (summaries are used instead of titles since they tend to be longer)
* `Generating clouds`: generates a wordcloud for each source, as well as the full dataset

###### RenderCloud Class

Next, we can define the class `RenderCloud` for the `wordcloud` task. `RenderCloud` has 3 methods:

* `word_cloud`: generates a wordcloud visualisation
* `setup`: sets the order of steps to run
* `run`: defines what each step does during the run

???+ attention
     `word_cloud` is a utility method for this task, while `setup` and `run` are the usual SAYN methods. Please note that methods `setup` and `run` need to return either `self.success()` or `self.fail()` in order to run.

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

#### Task Details (`nlp`)

The `nlp` task will have the following steps:

* `Processing texts`: loops through text_fields, generates text statistics on each entry
* `Updating database`: similar to LoadData step, has additional debugging information

###### LanguageProcessing Class

Moving on, we can define the class `LanguageProcessing` for the `nlp` task. `LanguageProcessing` has 3 methods:

* `desc_text`: provides counts of letters, words and sentences in an article
* `setup`: sets the order of steps to run
* `run`: defines what each step does during the run

???+ attention
     `desc_text` is a utility method for this task, while `setup` and `run` are the usual SAYN methods. Please note that methods `setup` and `run` need to return either `self.success()` or `self.fail()` in order to run.

??? example "python/nlp.py"
    ```python
    import pandas as pd
    from sayn import PythonTask
    from nltk import download
    from nltk.tokenize import word_tokenize, sent_tokenize

    download("punkt")


    class LanguageProcessing(PythonTask):
        def desc_text(self, df, text_field, language):
            """Text stats generating function"""

            # counts the number of letters in text_field

            df[text_field + "_letters"] = df[text_field].fillna("").str.len()

            # counts the number of words in text_field

            df[text_field + "_words"] = (
                df[text_field]
                .fillna("")
                .apply(lambda x: len(word_tokenize(x, language=language)))
            )

            # counts the number of sentences in text_field

            df[text_field + "_sentences"] = (
                df[text_field]
                .fillna("")
                .apply(lambda x: len(sent_tokenize(x, language=language)))
            )

        def setup(self):
            self.set_run_steps(["Processing texts", "Updating database"])
            return self.success()

        def run(self):

            with self.step("Processing texts"):

                table = self.parameters["user_prefix"] + self.task_parameters["table"]
                text_fields = self.parameters["text"]

                df = pd.DataFrame(self.default_db.read_data(f"SELECT * FROM {table}"))

                for t in text_fields:
                    self.info(f"Processing texts for {t} field")
                    self.desc_text(df, t, "english")

            with self.step("Updating database"):
                if df is not None:

                    output = f"{table}_{self.name}"
                    n_rows = len(df)
                    self.info(f"Loading {n_rows} rows into destination: {output}....")
                    df.to_sql(
                        output, self.default_db.engine, if_exists="replace", index=False
                    )

            return self.success()

    ```

#### Task Details (`dim_bbc_feeds_nlp_stats`)

Now that we have individual article statistics, it would be a good idea to create an additional modelling task to find some aggregate statistics grouped by source. Let's create another SQL query called `dim_bbc_feeds_nlp_stats` in the `sql` folder. This query will give us the average, grouped by source, of the text statistics generated by the `nlp` task.

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
