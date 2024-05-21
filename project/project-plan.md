# Project Plan

## Summary

<!-- Describe your data science project. -->

This data science project aims to analyze **the weather and climate conditions in New York alongside its road traffic volume** by using data from several automatic figuring out terminalss across the city. The objective is to evaluate whether New York is a suitable city for vehicle users. Two open data sources will be utilized: [dataGov](https://catalog.data.gov/dataset) for information on road traffic volume in New York and [visualCrossing](https://www.visualcrossing.com/weather/weather-data-services/) for weather and climate data. The analysis will focus on identifying patterns and trends in road traffic volume throughout 2012 to assess the city's suitability for road traffic. Additionally, the weather and climate data will be examined to determine if New York's conditions are favorable for road traffic.

## Rationale

<!-- Outline the impact of the analysis. Which problems it solves. -->
Analyzing weather, climate conditions, and road traffic volume in New York can be highly beneficial for several key audiences:

1. **Vehicle Enthusiasts:** This analysis can assist vehicle enthusiasts in determining whether New York fits their lifestyle by offering insights into how road traffic varies with different weather conditions. It helps reduce uncertainties regarding traffic scenarios and aids in better planning of routes and activities.

2. **Tourists Driving in the City:** Tourists can gain a lot from understanding how road traffic correlates with weather patterns. This knowledge can help them plan their travel itineraries more effectively, enhancing their overall experience and potentially boosting tourism in New York.

3. **City Planners in New York:** City planners can utilize the findings from this analysis to improve road traffic infrastructure's quality and safety. By pinpointing high and low traffic areas, they can make informed decisions about where to allocate resources and make improvements, ultimately making the city's roadways more user-friendly and efficient.

In summary, this analysis aims to decrease uncertainties about traffic conditions for vehicle enthusiasts, offer city planners valuable data for infrastructure improvements, and provide tourists with crucial information for better travel planning in New York.


## Datasources

<!-- Give a sectional overview of each data source you intend to use. Put "DatasourceX" in the prefic, where X is the datasource's id.  -->


### Datasource1: Road Traffic Volume Data in New York
* Metadata URL: [https://catalog.data.gov/dataset/traffic-volume-counts](https://catalog.data.gov/dataset/traffic-volume-counts)
* Sample Data URL: [https://data.cityofnewyork.us/api/views/7ym2-wayt/rows.csv ](https://data.cityofnewyork.us/api/views/7ym2-wayt/rows.csv )



### Datasource2: Weather and Climate Data of New York
* Metadata URL: [https://www.visualcrossing.com/weather/weather-data-services/New%20York?v=api](https://www.visualcrossing.com/weather/weather-data-services/New%20York?v=api)
* Sample Data URL: [  https://data.cityofnewyork.us/api/views/btm5-ppia/rows.csv](https://data.cityofnewyork.us/api/views/btm5-ppia/rows.csv )
* Data Type: CSV

This data source will provide weather and climate data of New York city, including temperature, daily minimum and maximum air temperature, yearly precipitation total, maximum snow depth, average wind direction and speed, peak wind gust, average sea-level air pressure, and monthly sunshine total.

## Work Packages

<!-- Work packages listed in descending order, each referring to a more detailed description of the issue. -->

1. Retrieve information from several sources. [#1][i1]
2. Execute the ETL pipeline's Data Transformation step. [#2][i2]
3. Put the ETL data pipeline's data loading step into action. [#3][i3]
4. Project-Specific Automated Testing. [#4][i4]
5. Project's Pipeline for Continuous Integration. [#5][i5]
6. Submission of the Complete Report and Presentation. [#6][i6]

[i1]: https://github.com/prosenjit-chd/MdAdDaEgg_FAU_Summer2024_Prosenjit_Chowdhury_23361276_made-template/issues/1
[i2]: https://github.com/prosenjit-chd/MdAdDaEgg_FAU_Summer2024_Prosenjit_Chowdhury_23361276_made-template/issues/2
[i3]: https://github.com/prosenjit-chd/MdAdDaEgg_FAU_Summer2024_Prosenjit_Chowdhury_23361276_made-template/issues/3
[i4]: https://github.com/prosenjit-chd/MdAdDaEgg_FAU_Summer2024_Prosenjit_Chowdhury_23361276_made-template/issues/4
[i5]: https://github.com/prosenjit-chd/MdAdDaEgg_FAU_Summer2024_Prosenjit_Chowdhury_23361276_made-template/issues/5
[i6]: https://github.com/prosenjit-chd/MdAdDaEgg_FAU_Summer2024_Prosenjit_Chowdhury_23361276_made-template/issues/6
