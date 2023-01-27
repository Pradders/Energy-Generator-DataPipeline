# Energy-Generator-DataPipeline
A data pipeline calculating the total energy output (in MW) of all electricity generators throughout November 2021.

# General inspiration
This system aims to create an ETL pipeline to ultimately calculate the total revenue earned by various wind and solar energy generators around Australia within November 2021. This required extracting, cleaning and integrating various datasets from online together as part of an overall calculation.

Power readings in MW were provided in 5-min dispatch intervals throughout the month of November in 2021. This was correlated with the associated Marginal Loss Factor (MFL), which represents losses during energy transportation for that specific generator during that dispatch period, as well as the Regional Reference Price (RRP), which represents energy (selling) prices in particular states/territories around Australia where the generators are located.

Revenue (in AUD) of each energy generator is calculated by multiplying the generator electrical energy output (in MWh) with MFL (dimensionless), and with RRP (in AUD/MWh). The 5-min dispatch period was crucial to convert the power readings from MW to MWh and it was assumed that the energy readings remained constant during each interval.

These are important considerations to make, as combined, these could ultimately suggest (if, for example, the revenue is low) that a particular location is not suitable to cheaply collect much wind or solar-derived energy and transport it efficiently/closely enough to avoid energy losses during transmission.

This generally required collecting data from online, converting them to dataframes, cleaning them by keeping only relevant values, reprocessing data (e.g., date/time values), combining dataframes to maintain key correlations and generator descriptions, calculating the revenue for each generator based on time differentials, summing them together and exporting the final dataframe as a csv file.

The output file was labelled as 'output.csv'.

The script can simply be run without any alterations or additional inputs, as everything will be automatically be processed and downloaded into the current directory of the script.
