# Data Processing for MTN Momo Data

As stated above, this repository contains an xml file containing 1600 messages from MTN Momo (data) as well as code to extract the data from the xml file, sort it into different categories, then extracting information from these messages and storing it in an SQLite database.

## Installation

1. Download the repository from github using the link or the command

     git clone https://github.com/octaviusthe3rd/summative_group_47.git

2. Navigate into the downloaded repository
3. At the root of the repository, activate the virtual environment using one of the below commands
 
    Windows command:

            dev_env/Scripts/activate

    Mac/Linux command:

            source myenv/bin/activate

4. Install the project dependecies using the following command

            pip install -r requirements.txt

## How to run the program

1. Navigate to './sort'
2. Run the python file 'sort.py' to sort the messages into different categories
    - The results will be stored as txt files in the directory 'separate_files'
3. From the root of the folder navigate to './separate_files'
4. Run the python file 'regex.py' to parse all the txt files to json files
    - The results will be stored in under the newly created directory 'parsed_json'
5. From the root of the folder navigate to './db'
6. Run the file 'load-db.py' to generate the database and load the data from the json files to their respective fields 