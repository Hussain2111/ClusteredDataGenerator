import { faker } from '@faker-js/faker';
import * as fs from 'fs/promises';
import * as xlsx from 'xlsx';
import * as readline from 'readline';

type ColumnInfo = {
    DataType: string;
    Format?: string;
    Order: string;
    ExcelColumnName: string;
    IsDate: string;
    IsDouble: string;
    IsInteger: string;
};

type JsonStructure = ColumnInfo[];

const checkDuplicateOrders = (structure: JsonStructure): void => {
    const orderNumbers = new Set<string>();
    structure.forEach(column => {
        if (orderNumbers.has(column.Order)) {
            throw new Error(`Duplicate order number found: ${column.Order}`);
        }
        orderNumbers.add(column.Order);
    });
    console.log('Duplicate order check passed.');
};

const generateRandomValue = (column: ColumnInfo): any => {
    switch (column.DataType) {
        case 'String':
            const lengthLimit = parseInt(column.Format || '50');
            return faker.string.alpha({ length: { min: 1, max: lengthLimit } });
        case 'Numeric':
            if (column.IsInteger === '1') return faker.number.int();
            if (column.IsDouble === '1') return Number(faker.number.float().toFixed(2));
            return faker.number.float();
        case 'Date':
            return column.Format === 'MM/DD/YYYY'
                ? faker.date.past().toLocaleDateString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric' })
                : faker.date.past().toISOString();
        default:
            return null;
    }
};

const createPersonObject = (structure: JsonStructure): Record<string, any> =>
    structure.reduce((person, column) => {
        person[column.ExcelColumnName] = generateRandomValue(column);
        return person;
    }, {} as Record<string, any>);

const saveToExcel = (data: any[], filePath: string) => {
    const workbook = xlsx.utils.book_new();
    const worksheet = xlsx.utils.json_to_sheet(data);
    xlsx.utils.book_append_sheet(workbook, worksheet, 'Sheet1');
    xlsx.writeFile(workbook, filePath);
};

const generateData = (jsonStructure: JsonStructure, recordsToGenerate: number) =>
    Array.from({ length: recordsToGenerate }, (_, index) => {
        if (index % 1000 === 0) console.log(`Generating record ${index + 1} of ${recordsToGenerate}`);
        return createPersonObject(jsonStructure);
    });

const main = async () => {
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });

    rl.question('How many records do you want to generate? ', async (answer) => {
        const numRecords = parseInt(answer);
        if (isNaN(numRecords) || numRecords <= 0) {
            console.log('Please enter a valid positive number.');
            rl.close();
            process.exit(1);
        }

        console.log(`Generating ${numRecords} records...`);

        try {
            const data = await fs.readFile('input.json', 'utf8');
            const jsonStructure: ColumnInfo[] = JSON.parse(data);
            checkDuplicateOrders(jsonStructure);
            jsonStructure.sort((a, b) => parseInt(a.Order) - parseInt(b.Order));

            const persons = generateData(jsonStructure, numRecords);
            saveToExcel(persons, 'output.xlsx');

            console.log(`${numRecords} records generated and saved to output.xlsx`);
        } catch (error) {
            console.error("Error processing data:", error);
        } finally {
            rl.close();
            process.exit(0);
        }
    });
};

main();
