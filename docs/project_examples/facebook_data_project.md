# SAYN Project Example: Facebook Data Project

## Project Description

#### Overview

This is an example SAYN project which shows how to use SAYN for data modelling and processing. You can find the GitHub repository
[here](https://github.com/173TECH/facebook_data_project){target="\_blank"}.

This project does the following:

* Extracts Facebook Messenger data
* Loads it into a SQLite database
* Cleans the extracted data
* Calculates reply times for chat data
* Performs some basic text and sentiment analysis on the transformed data
* Generates wordcloud timelapse GIFs for each conversation
* Generates a bar chart race GIF for most shared sites in chat data

![`Bar Chart Race Example`](chart_race.gif)


#### Features Used

* [Python tasks](../tasks/python.md) to extract and analyse data
* [Autosql tasks](../tasks/autosql.md) to automate SQL transformations.
* Usage of [parameters](../parameters.md) to make the code dynamic.
* Usage of [presets](../presets.md) to define tasks.

In addition to SAYN, this project uses the following packages:

* Data processing: `numpy`, `pandas`, `nltk`, `vaderSentiment`
* Visualisations: `matplotlib`, `wordcloud`, `pillow`, `bar_chart_race`

By default, the project uses SQLite as a database. You can use [DB Browser for SQLite](https://sqlitebrowser.org/dl/){target="\_blank"} to navigate the data easily. You can also connect this database to your preferred visualisation tool.


#### Running The Project

To run the project, you will need to:

1. clone the repository with `git clone https://github.com/173TECH/facebook_data_project.git`.
2. rename the `sample_settings.yaml` file to `settings.yaml`.
3. install the project dependencies by running the `pip install -r requirements.txt` command from the root of the project folder.
4. install `ImageMagick`, details here: https://imagemagick.org/
5. use `sayn run` from the root of the project folder to run all SAYN commands.

???+ attention
     This project comes with a sample dataset, you should use this dataset to test run the project.
     After a successful run you should see 3 new files in `python/img`, these should be the following:

     - sample_Goku_timelapse.gif
     - sample_Vegeta_timelapse.gif
     - chart_race.gif

<br>

### Adding Your Facebook Messenger Data

For this you will need your Facebook Messenger data in JSON format, you can get request it by doing the following:

1. Sign in to Facebook
2. Go to Settings & Privacy > Settings > Your Facebook Information > Download Your Information
3. Change format to JSON and click Create File (this can take a while depending on your date range and media quality)

Once you have the data, you can find the chat data in `messages/inbox` (you should see a collection of folders corresponding to each of your chats):

1. Copy and paste the chat folders you are interested into the `data` folder in this project.
2. In `tasks/data_science.yaml`, change the `facebook_name` parameter to your full name on Facebook

Note: If you use a large amount of chat data you will experience longer load times for certain tasks

???+ note
     If you use a large amount of chat data you will experience longer load times for certain tasks
