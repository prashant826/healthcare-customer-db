-- Table query sql

CREATE TABLE Table_Country (
    Customer_Name VARCHAR(255),
    Customer_Id VARCHAR(18) PRIMARY KEY,
    Open_Date DATE,
    Last_Consulted_Date DATE,
    Vaccination_Id VARCHAR(255),
    Dr_Name VARCHAR(255),
    State VARCHAR(5),
    Country VARCHAR(5),
    DOB DATE,
    Is_Active CHAR(1),
    Age INT,
    Days_Since_Last_Consulted INT
);