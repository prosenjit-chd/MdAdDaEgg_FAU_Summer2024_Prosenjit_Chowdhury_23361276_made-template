# Exercise Badges

![](https://byob.yarr.is/prosenjit-chd/MdAdDaEgg_FAU_Summer2024_Prosenjit_Chowdhury_23361276_made-template/score_ex1) ![](https://byob.yarr.is/prosenjit-chd/MdAdDaEgg_FAU_Summer2024_Prosenjit_Chowdhury_23361276_made-template/score_ex2) ![](https://byob.yarr.is/prosenjit-chd/MdAdDaEgg_FAU_Summer2024_Prosenjit_Chowdhury_23361276_made-template/score_ex3) ![](https://byob.yarr.is/prosenjit-chd/MdAdDaEgg_FAU_Summer2024_Prosenjit_Chowdhury_23361276_made-template/score_ex4) ![](https://byob.yarr.is/prosenjit-chd/MdAdDaEgg_FAU_Summer2024_Prosenjit_Chowdhury_23361276_made-template/score_ex5)

# Connection between Weather and Traffic volume number in New York city

The main goal of this project is to integrate and analyze traffic volume and weather data for New York City to evaluate the city's traffic management strategies and their effectiveness in different weather conditions. By examining trends in traffic counts from the New York City dataset from 2012 and the city's weather patterns, the project aims to determine if New York City's infrastructure and climate support an efficient and manageable traffic flow. This analysis will provide insights into how weather conditions impact traffic volumes and will help in developing strategies to improve traffic management throughout the year.
This project have two data sets: [dataGov](https://catalog.data.gov/dataset) for information on road traffic volume in New York and [Meteostat](https://meteostat.net/en/) for weather and climate data.


## Project Structure

```bash
project/             
├── analysis-report.pdf             
├── data-report.pdf               
├── pipeline.py                    
├── pipeline.sh                    
├── project-plan.md                        
└── test_pipeline.py             
└── test.sh             
```

**Important files of the project and their roles:**

- `project/data-report.pdf`: This PDF provides a detailed description of the dataset utilized in our project and elaborates on the primary objectives of the project.
- `project/pipeline.py`: It will execute an automated pipeline designed to create an SQLite database. `MADE.sqlite` which includes two tables representing two open data sources utilized in the project.
- `project/test.sh`: A Bash script will perform component and system-level testing for the project by invoking two additional Python scripts. `project/test_pipeline.py`.
- `project/analysis-report.pdf`: This PDF details the final findings of the project, offering a comprehensive exploration of all aspects.

**Project Pipeline using GitHub Action:** <br>

A project pipeline has been established utilizing a GitHub Action defined in [.github/workflows/pipeline-test.yml](.github/workflows/pipeline-test.yml). This pipeline is activated whenever modifications are made to the `project/` directory and pushed to the GitHub repository, or when a pull request is created and merged into the `main` branch. The `pipeline-test.yml` workflow runs the `project/test.sh` test script and, if any failures occur, an error message is dispatched.

## Project Setup

1. Clone this git repository
```bash
git clone https://github.com/SK-Subroto/fau-made-template-ss24
```
2. Go to project directory
```bash
cd project
```
3. Install [Python](https://www.python.org/). Then create a virtual environment inside the repo and activate it.
```bash
python -m venv venv
.\venv\Scripts\Activate.ps1 
```
4. Install the required Python packages for the project.
```bash
pip install pandas requests
```
5. Run the project
```bash
python pipeline.py
```
6. Run the test script
```bash
chmod +x test.sh
sh test.sh
```



## Exercises (No connection with project)
During the semester you will need to complete exercises using [Jayvee](https://github.com/jvalue/jayvee). You **must** place your submission in the `exercises` folder in your repository and name them according to their number from one to five: `exercise<number from 1-5>.jv`.

In regular intervalls, exercises will be given as homework to complete during the semester. Details and deadlines will be discussed in the lecture, also see the [course schedule](https://made.uni1.de/). At the end of the semester, you will therefore have the following files in your repository:

1. `./exercises/exercise1.jv`
2. `./exercises/exercise2.jv`
3. `./exercises/exercise3.jv`
4. `./exercises/exercise4.jv`
5. `./exercises/exercise5.jv`

### Exercise Feedback
We provide automated exercise feedback using a GitHub action (that is defined in `.github/workflows/exercise-feedback.yml`). 

