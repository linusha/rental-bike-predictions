# Tools

## Environment and Packages

This project uses `conda` for managing virtual environments and `pip` to handle dependencies.

### Setup

```sh
# Create a new conda env for the project, using python 3.11 and installing pip in it
conda create -n bike-project python=3.11 pip
conda activate bike-project
pip install -r requirements.txt
```

To ensure that you have all data available locally, after your first pull, you'll need to run

```sh
python combine_trip_weather_data.py
```

at the root of this repository and with the environment you just created activated.

### Installing Dependencies

From the root of the repository run:
```sh
pip install -r requirements.txt
```

### Adding Dependencies

When you want to add a dependency to the project:

1. Install the dependency by using `pip install`.
2. Run `pip freeze > requirements.txt`, from the root of the repository.
3. Make sure to commit the updated `requirements.txt` file along with the changes that required the new dependency.


## Data Sources

This project relies on a combination of data sources:

- [This](https://www.kaggle.com/datasets/archit9406/bike-sharing) bike-rental data set from kaggle, that provides us with daily weather data for 2011-2012
- More granular data for bike-rides from [capital bikeshare](https://capitalbikeshare.com/system-data)