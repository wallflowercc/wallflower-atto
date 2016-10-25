# Wallflower.Atto Server

The Wallflower.Atto server implements a simple HTTP API for storing and retrieving data points (timestamped integers, floats, and strings) and includes a JS/jQuery web interface. The server is written in Python and the HTTP API is built upon a Flask app. By modifying the wallflower_config.json file, the server can be set to use a PostgreSQL or SQLite database for storage. We have elected to leave the API exposed in the server, demo, and interface code to enable easy modification and experimentation.

#### Quick Start Guide

Wallflower.Atto requires Flask and Flask-SQLAlchemy, which can be installed with pip
```sh
$ pip install Flask Flask-SQLAlchemy
```
To start the server on your computer, run
```sh
$ python wallflower_atto_server.py
```
Open a web browser and navigate to http://127.0.0.1:5000/ to view the interactive dashboard. By default, the server will also be publicly available on your network and accessible via the IP address of your computer (i.e. http://IP_ADDRESS:5000/).

The wallflower_demo.py file includes sample Python code for creating objects and streams and for sending new data points. The Wallflower.Atto server is still in beta development, so if you find a bug, please let us know.

#### Deploying to Heroku

The Wallflower.Atto server can be deployed to the Heroku cloud application platform with the following steps.

##### Create Accounts
- Setup a [Heroku] account. It's free. 
- Setup a [Cloud9] account. It's free. We will use Cloud9 to edit and deploy the app.

##### Clone The App
 - Create a new workstation on Cloud9 (c9.io) and use the Custom template. This will take a minute as Cloud9 creates a virtual machine.
 - Use the console at the bottom of the page to clone the app into your workstation.
 - Note: Only enter the text that comes after $.
 - Note: /wallflower-atto $ means you must run the command from the wallflower-atto directory.
```sh
$ git clone https://github.com/wallflowercc/wallflower-atto
```
 - You now have your own copy of the Wallflower.Atto server which you can edit from the Cloud9 workstation. Edit the wallflower_config.json file to select the Heroku  PostgreSQL database type by changing

 ```sh
"type": "sqlite",
```
to
```sh
"type": "postgresql-heroku",
```
 
##### Connect To Heroku
 - Next we will login to Heroku from Cloud9.
```sh
$ heroku login
```

##### Create a New Heroku App with a PostreSQL Database
```sh
$ cd wallflower-atto/
/wallflower-atto $ heroku create
/wallflower-atto $ heroku addons:create heroku-postgresql:hobby-dev
/wallflower-atto $ heroku ps:scale web=1
```

##### Update the Git Repository
```sh
/wallflower-atto $ git add --all
/wallflower-atto $ git commit -m "Initial Save"
```

##### Push the App to Heroku
```sh
/wallflower-atto $ git push heroku master
```
 - That is it. The Wallflower.Atto server is now running on Heroku. Below are a few more notes. 
 
### Run the Wallflower.Atto server on Cloud9
 - Install the necessary Python modules on the Cloud9 workstation.
```sh
$ pip install Flask Flask-SQLAlchemy gunicorn psycopg2
```
 - To run the app from Cloud9, login to Heroku from the Cloud9 console and run
```sh
/wallflower-atto $ DATABASE_URL=$(heroku config:get DATABASE_URL) heroku local
```
 - The app will be running at https://WORKSPACE-USERNAME.c9users.io/ where WORKSPACE is the name of your workspace and USERNAME is your Cloud9 username. Use CTRL+C to close the app.
 
 
##### Saving Changes and Pushing to Heroku
 - After making changes to your app, enter these commands to push the changes to Heroku.
```sh
$ cd nano-db-for-heroku/
/nano-db-for-heroku $ git add --all
/nano-db-for-heroku $ git commit -m "Message Describing Changes"
/nano-db-for-heroku $ git push heroku master
```


#### License

The Wallflower.Pico source code is licensed under the [AGPL v3][agpl]. You can find a reference to this license at the top of each source code file.

Components which connect to the server via the API are not affected by the AGPL. This extends to the Python example code and the HTML, JS, and CSS code of the web interface, which are licensed under the [MIT license][mit].

In summary, any modifications to the Wallflower.Pico source code must be distributed according to the terms of the AGPL v3. Any code that connects to a Wallflower.cc server via an API is recognized as a seperate work (not a derivative work) irrespective of where it runs. Lastly, you are free to modify the HTML, JS, and CSS code of the web interface without restrictions, though we would appreciate you sharing what you have created.


[wcc]: <http://wallflower.cc>
[wccdemo]: <http://wallflower.cc/pico-demo>
[mit]: <https://opensource.org/licenses/MIT>
[agpl]: <https://opensource.org/licenses/AGPL-3.0>
[Heroku]: <https://www.heroku.com/>
[Cloud9]: <https://www.c9.io/>
