# **Data Dictionary**
## Immigration Table by Year, Month, State, City
 * cicid: Individual ID number (float)
 * year: Year of Immigration record (integer)
 * month: Month of Immigration record (integer)
 * i94port: City Port Code where Immigrant entered (String)
 * state: State where Immigrant entered (String)
 * biryear: Person's year of birth (integer)
 * gender: Gener of Person (string)
 * airline: Air Line code person used for entery (string)
 * visatype: The type of visa the person is on for entry to the country (string)

## US Temperature Table by by Year, Month, State, City
 * year: Year the temperature was recorded (integer)
 * month: Month the temperature was recorded (integer)
 * City: Name of City (string)
 * Country: Name of Country. Filtered for only 'United States' (string)
 * avg_temp_celcius: Avg Temperature in Celcius per City (float)
 * avg_temp_fahrenheit: Avg Temperatrue in Fahrenheit per City (float)


## US Airports Table by State, City
 * Country: Country of Airport. Filtered to United States (string)
 * State_Code: Code ID for each State of the US. (string)
 * City: City Location of the airport (string)
 * elevation_ft:  Elevation of the airport location in feet (float)
 * avg_elevation_ft: double (nullable = true)-Average elevation based on all airport data from each state. (float)
 * type: The airport type (string)
 * name: The name of the airport (string)
 * ident: The ID code of the airport (string)
  
## US Demographic Table by State, City
 * City: Name of the city (string)
 * State: Full name of the US State (sting)
 * State_Code: Abbreviated code of the State (string)
 * Median_Age: TheMedian Age per city (float)
 * %_Male: % of males in the city population
 * %_Female: % of females in the city population
 * %_Veterans: % of males in city population
 * %_Foreign_Born: % of foreign borns in city population
 * Native_American: Count of Native American people in city population
 * Asian: Count of Asian people in city population
 * Black: Count of Black people in city population
 * Hispanic_or_Latino: Count of Hispanic or Latino Latino people in city population
 * White: Count of White people in city population