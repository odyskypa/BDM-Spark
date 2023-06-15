# Big Data Management - Universitat Politècnica de Catalunya
## Implementation of a (Big) Data Management Backbone
### Barcelona Rentals - Territorial Income Distribution - Age of Buildings 
***
### Instructions for Executing the Code

- Download Python version 3.10
- Clone this repository locally
  - ``` git clone git@github.com:odyskypa/BDM-Spark.git```
- Create a virtual environment with Python 3.10 interpreter and run the following command to install the required libraries:
  - ```
      pip install requirements.txt
    ```
- Add a `.env` file inside `BDM-Spark` folder, including the following parameters with their appropriate values:
  - ```
    VM_HOST="..."
    VM_USER="..."
    MONGODB_PORT=27017
    PERSISTENT_DB="persistent"
    FORMATTED_DB="formatted"
    ```
- The project is divided into 3 main parts:
  - [The Data Formatting Processes](https://github.com/odyskypa/BDM-Spark/blob/main/src/data_formatters/data_formatter.py)
  - [The Descriptive Analytics](https://github.com/odyskypa/BDM-Spark/blob/main/src/descriptive_analysis/data_description.py)
  - [The Predictive Analytics](https://github.com/odyskypa/BDM-Spark/blob/main/src/predictive_analysis/data_modeling.py)
- For executing the `Data Formatting` processes in order to `fix data-types`, `drop-duplicates` and `reconcile` the data sources
make use of the following commands, with the respective order:
  - ``` 
    python main.py data-formatting merge-lookup-tables
    python main.py data-formatting fix-data-types
    python main.py data-formatting drop-duplicates
    python main.py data-formatting reconcile-data
    ```
- Moreover, for executing the `Descriptive Analytics` functionality in order to ...., use the following command:
  - ```
    python main.py data-description
    ```
- Lastly, for executing the `Predictive Analytics` processes, follow the commands below:
  - ```
      python main.py data-prediction
      ```

### Limitations

