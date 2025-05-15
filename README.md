# Bike Sharing Rental Prediction in D.C.

This repository contains code, results and supplementary materials for our project developed as part of the "Machine Learning" course (Summer 2025) in the [Master of Data Science for Public Policy](https://www.hertie-school.org/en/mds) at the [Hertie School](https://www.hertie-school.org).

The project was made by

- [Mona Borth](https://github.com/mona-borth)
- [Luis Windpassinger](https://github.com/lwndp)
- [Linus Hagemann](https://github.com/linusha)

> [!NOTE]  
> For more context on what we did and why we did it, please take a look at `report.pdf`.
> The slides used for the project presentation in the course can be found in `slides.pdf`.

## Structure and Data Management

### Data Sources

This project relies on a combination of data sources:

- Granular data for bike-rides from [capital bikeshare](https://capitalbikeshare.com/system-data)
- Weater data from the [Open-Meteo API](https://open-meteo.com/en/docs)
- Information about stations in the capital bikeshare network, retrieved from [OpenData D.C.](https://opendata.dc.gov/datasets/a1f7acf65795451d89f0a38565a975b3_5/about)

The station data can be found inside of the `data/raw` folder. Weather data can either be retrieved manually using the script under `data/get_weather_data.py`, or you can utilize our download of the 2023 data under `data/processed/weather_hourly_all_locations_2023.parquet`. Trip data for 2023 need to be manually retrieved from the above mentioned URL. 

> [!IMPORTANT]  
> All of this is only necessary if you want to run our data preprocessing script (`data/data-merging-preprocessing-engineering.ipynb`) from the ground up. **Usually, you will not need to do this.** All of our models can be run and replicated solely relying on the appropriate scripts and relying on our final dataframe to be found under `data/final/df.parquet`.

### Scripts and Contents of this Repository

The scripts relevant for the main analysis of the project can be found in `eda.ipynb`, `clustering.ipynb`, and `main-modelling.ipynb`.

Execution of the `main-modelling` notebook can take a very long time and be quite demaning of your RAM. We recommend setting appropiate values inside of `CONFIG` block at the very top of the notebook and only executing the modelling blocks one-by-one, depending on your needs.

Before running any models, source all functions definitions defined above the `Training Loops` section of the notebook.

Figures and eveluation metrics of the models are automatically stored in the `figures` and `checkpoints` directories respectively.

## Environment and Packages

This project uses `conda` for managing virtual environments and `pip` to handle dependencies.

### Setup

```sh
# Create a new conda env for the project, using python 3.11 and installing pip in it
conda create -n bike-project python=3.11 pip
conda activate bike-project
pip install -r requirements.txt
```

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