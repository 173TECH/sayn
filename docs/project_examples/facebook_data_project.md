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
* Generates a photo mosaic image from chat photos
* Generates and automatically fills YouTube playlists based on video links in the chat data


#### Features Used

* [Python tasks](../tasks/python.md) to extract and analyse data
* [Autosql tasks](../tasks/autosql.md) to automate SQL transformations.
* Usage of [parameters](../parameters.md) to make the code dynamic.
* Usage of [presets](../presets.md) to define tasks.

In addition to SAYN, this project uses the following packages:

* Data processing: `numpy`, `pandas`, `nltk`, `vaderSentiment`
* Visualisations: `matplotlib`, `wordcloud`, `pillow`, `bar_chart_race`
* YouTube Data API connection: `google-auth-oauthlib`, `google-api-python-client`

By default, the project uses SQLite as a database. You can use [DB Browser for SQLite](https://sqlitebrowser.org/dl/){target="\_blank"} to navigate the data easily. You can also connect this database to your preferred visualisation tool.


#### Running The Project

1. Clone the repository with the command `git clone https://github.com/173TECH/facebook_data_project`.
2. Rename the `sample_settings.yaml` file to `settings.yaml`.
3. Install the project dependencies by running the `pip install -r requirements.txt` command from the root of the project folder.
4. Add your Facebook Messenger data
5. Perform some additional task specific requirements, details below.  
6. Run all SAYN commands from the root of the project folder.

<br>

### Adding Your Facebook Messenger Data

To run this project you will need your Facebook Messenger data in JSON format, you can request it from Facebook by doing the following:

1. Sign in to Facebook
2. Go to Settings & Privacy > Settings > Your Facebook Information > Download Your Information
3. Change format to JSON and click Create File (this can take a while depending on your date range and media quality)

Once you have the data:

1. Copy the folder called `inbox` inside the `messages` subfolder and paste it inside the `python` folder of the project
2. Rename the pasted folder to `messenger_data`

???+ attention
     Depending on the size of data, you might want to delete some chats from your `messenger_data` folder to speed up your task run times.

<br>

### Additional Task Specific Requirements

#### `wordcloud`

In `tasks/data_science.yaml`, parameter `facebook_name` should be changed to your name on facebook  

#### `photo_mosaic`

In `tasks/data_science.yaml`, parameter `user_data` should be changed to the name of any subfolder in `messenger_data`.

#### `link_chart_race`

[ImageMagick](https://imagemagick.org/){target="\_blank"} needs to be installed on your device.

#### `youtube_playlists`

In `tasks/data_science.yaml`:

*  Parameter `user` should be changed to the name of any subfolder in `messenger_data`.
*  Parameter `category` can be changed to other types of content, the default is music.

Requires YouTube Channel and Google OAuth 2.0 credentials.

To create a YouTube Channel:

1. Sign in to YouTube.
2. Click on your profile picture.
3. Create a channel.

To create Google OAuth 2.0 credentials:

1. Create a project in the [Google Developers Console](https://console.developers.google.com/){target="\_blank"}
2. Go to [Credentials page](https://console.developers.google.com/apis/credentials){target="\_blank"}
3. Click + CREATE CREDENTIALS > OAuth client ID > Application type > Desktop app > CREATE
4. Enable the YouTube Data API v3 on the project.

??? "How to enable the YouTube Data API v3"
     1. Go to [Enabled APIs page](https://console.developers.google.com/apis/enabled){target="\_blank"}
     2. Click + ENABLE APIS AND SERVICES
     3. Search for "YouTube Data API v3"
     4. Click ENABLE (if already enabled it should say MANAGE)

After you have created your OAuth 2.0 credential:

1. Download them from the [Credentials page](https://console.developers.google.com/apis/credentials){target="\_blank"} and rename them to `client_secrets.json`.
2. Move `client_secrets.json` to the `sample_secrets` folder in your project.
3. Rename the `sample_secrets` folder to `secrets`.

???+ attention
     When running this task, you will get a prompt to give permissions to your project.

??? tip
    You can exclude a specific task from a full run by using `sayn run -x task_name`. To exclude multiple tasks, you can chain task names using `-x`, e.g. `sayn run -x task_name_1 -x task_name_2`
