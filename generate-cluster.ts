import {faker} from '@faker-js/faker';
import ExcelJS from 'exceljs';
import { promises as fsPromises } from 'fs';
import { exit } from 'process';
import readline from 'readline';

type ColumnInfo = { 
    DataType: string,
    Format: string,
    Order: string,
    ExcelColumnName: string,
    IsDate: string,
    IsDouble: string,
    IsInteger: string
}

type JsonStructure = ColumnInfo[];

const createPersonObject = (structure: JsonStructure): Record<string, any> => 
    structure.reduce((person, column) => {
    person[column.ExcelColumnName] = generateData(column);
    return person;
    },
    {} as Record<string, any>    
)

const generateRecords = (jsonStructure: JsonStructure, recordsToGenerate: number) =>
    Array.from({ length: recordsToGenerate }, () => createPersonObject(jsonStructure));

const generateData = (column: ColumnInfo) => {
    switch (column.DataType) {
        case 'String':
            const lengthLimit = parseInt(column.Format || '50');
            return faker.string.alpha({ length: { min: 1, max: lengthLimit } });
        case 'Numeric':
            if (column.IsInteger === '1') return faker.number.int();
            if (column.IsDouble === '1') return Number(faker.number.float().toFixed(2));
       case 'Date':
            return faker.date.recent().toISOString();
        default:
            return null;
    }
};

const checkDupOrders = (structure: JsonStructure): void => {
    const orders = new Set<string>();
    structure.forEach(column => {
        if(orders.has(column.Order)) {
            throw new Error(`Duplicate order: ${column.Order}`);
        }
        orders.add(column.Order);
    });
};

async function generateFile (jsonStructure: JsonStructure, totalRecords: number, filePath: string) {
    const workbook = new ExcelJS.stream.xlsx.WorkbookWriter({filename: filePath});
    const worksheet = workbook.addWorksheet('Sheet1');
    const chunkSize = 5000; // Using a fixed chunk size

    worksheet.addRow(jsonStructure.map(col => col.ExcelColumnName)).commit();

    const totalChunks = Math.ceil(totalRecords / chunkSize);

    for (let i = 0; i < totalChunks; i++) {
        const chunk = generateRecords(jsonStructure, Math.min(chunkSize, totalRecords - i * chunkSize));
        chunk.forEach(record => {
            worksheet.addRow(Object.values(record)).commit();
        });
        console.log(`Chunk ${i + 1} of ${totalChunks} generated`);
    }
    await workbook.commit();
}

const getUserInput = async (question: string): Promise<string> => {
    const rl = readline.createInterface({
        input: process.stdin,
        output: process.stdout
    });

    return new Promise((resolve) => {
        rl.question(question, (answer) => {
            rl.close();
            resolve(answer);
        });
    });
};

// Main execution
const main = async () => {
    try {
        const totalRecordsInput = await getUserInput('Enter the number of records to generate: ');
        const totalRecords = parseInt(totalRecordsInput);

        if (isNaN(totalRecords) || totalRecords <= 0) {
            throw new Error('Invalid input. Please enter a positive integer.');
        }

        const data = await fsPromises.readFile('input.json', 'utf8');
        const jsonStructure: ColumnInfo[] = JSON.parse(data);
        checkDupOrders(jsonStructure);
        jsonStructure.sort((a, b) => parseInt(a.Order) - parseInt(b.Order));

        await generateFile(jsonStructure, totalRecords, 'output.xlsx');
        console.log(`${totalRecords} records generated and saved to output.xlsx`);
        exit(0);
    } catch (error) {
        console.error("Error processing data:", error);
    }
};

<<<<<<< HEAD
main();
=======
main();
>>>>>>> f63ffe766ad61d05c7bf734fccdb74bbea405e79
