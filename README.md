# ClusteredDataGenerator

This TypeScript program generates data based on a provided JSON structure and ingests it into an Excel file using the Faker library. It is designed to handle dynamic changes to the data structure and efficiently generate large volumes of records by leveraging Node.js's clustering capabilities.

## Features

- **Dynamic Column Handling:** The program adjusts to added or removed columns in the `input.json` file.
- **Duplicate Order Number Handling:** Ensures uniqueness of order numbers.
- **String Length Handling:** Generates strings of specified lengths for 'String' data types.
- **User-Specified Record Count:** Allows the user to specify the number of records to generate.
- **Performance Optimization:** Utilizes Node.js clustering to enhance performance for large data sets.

## Requirements

- Node.js v20.14
- TypeScript
- Faker library
- `csv-Writer` library
- `cluster` module
