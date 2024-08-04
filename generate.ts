// Import required modules
import { faker } from '@faker-js/faker';
import * as fs from 'fs';
import cluster from 'cluster';
import os from 'os';
import { performance } from 'perf_hooks';
import { Transform } from 'stream';

// Record the start time for performance measurement
const startTime = performance.now();

// Constants for batch processing and string pool size
const BATCH_SIZE = 10000;
const STRING_POOL_SIZE = 10000;

// Define the structure for column information
type ColumnInfo = {
    DataType: string;
    Format?: string;
    Order: string;
    ExcelColumnName: string;
    IsDate: string;
    IsDouble: string;
    IsInteger: string;
};

// Create a pool of pre-generated strings for efficiency
const stringPool = Array.from({ length: STRING_POOL_SIZE }, () => faker.string.alpha({ length: { min: 1, max: 50 } }));
let stringPoolIndex = 0;

/**
 * Get the next string from the pre-generated pool
 * @returns {string} A random string from the pool
 */
function getNextString(): string {
    const value = stringPool[stringPoolIndex];
    stringPoolIndex = (stringPoolIndex + 1) % STRING_POOL_SIZE;
    return value;
}

/**
 * Generate a random value based on the column type
 * @param {ColumnInfo} column - The column information
 * @returns {any} A random value of the appropriate type
 */
const generateRandomValue = (column: ColumnInfo): any => {
    switch (column.DataType) {
        case 'String': return getNextString();
        case 'Numeric':
            if (column.IsInteger === '1') return faker.number.int();
            if (column.IsDouble === '1') return Number(faker.number.float().toFixed(2));
            return faker.number.float();
        case 'Date':
            return column.Format === 'MM/DD/YYYY'
                ? faker.date.past().toLocaleDateString('en-US', { month: '2-digit', day: '2-digit', year: 'numeric' })
                : faker.date.past().toISOString();
        default: return null;
    }
};

/**
 * Create a CSV row for a person
 * @param {Map<string, ColumnInfo>} structureMap - Map of column structures
 * @returns {string} A comma-separated string of values
 */
const createPersonObject = (structureMap: Map<string, ColumnInfo>): string => {
    const values: string[] = [];
    for (let i = 2; i <= structureMap.size + 1; i++) {
        const column = structureMap.get(i.toString());
        if (column) {
            const value = generateRandomValue(column);
            values.push(typeof value === 'string' && value.includes(',') ? `"${value}"` : String(value));
        }
    }
    return values.join(',');
};

/**
 * Generate data and stream it to the output
 * @param {Map<string, ColumnInfo>} structureMap - Map of column structures
 * @param {number} recordsToGenerate - Number of records to generate
 * @param {Transform} outputStream - Stream to write the output
 * @returns {Promise<void>}
 */
const generateAndStreamData = (structureMap: Map<string, ColumnInfo>, recordsToGenerate: number, outputStream: Transform): Promise<void> => {
    return new Promise((resolve) => {
        let recordsWritten = 0;
        let batch: string[] = [];

        // Write the current batch to the output stream
        function writeBatch() {
            if (batch.length > 0) {
                outputStream.write(batch.join('\n') + '\n');
                batch = [];
            }
        }

        // Generate and write records
        function writeRecord() {
            if (recordsWritten >= recordsToGenerate) {
                writeBatch();
                outputStream.end();
                resolve();
                return;
            }

            batch.push(createPersonObject(structureMap));
            recordsWritten++;

            if (batch.length >= BATCH_SIZE) {
                writeBatch();
            }

            setImmediate(writeRecord);
        }

        writeRecord();
    });
};

// Main execution block
if (cluster.isPrimary) {
    // Primary process
    const numCPUs = os.cpus().length;
    const numRecords = parseInt(process.argv[2]) || 1000;
    const recordsPerWorker = Math.ceil(numRecords / numCPUs);

    console.log(`Generating ${numRecords} records using ${numCPUs} workers`);

    // Read the input file and set up the output stream
    fs.promises.readFile('input.json', 'utf8')
        .then(data => {
            const jsonStructure: ColumnInfo[] = JSON.parse(data);
            jsonStructure.sort((a, b) => parseInt(a.Order) - parseInt(b.Order));
            const structureMap = new Map(jsonStructure.map(col => [col.Order, col]));

            const outputStream = fs.createWriteStream('output.csv', { flags: 'w' });
            const headers = jsonStructure.map(col => col.ExcelColumnName).join(',') + '\n';
            outputStream.write(headers);

            let completedWorkers = 0;
            let totalRecordsWritten = 0;

            // Create worker processes
            for (let i = 0; i < numCPUs; i++) {
                const worker = cluster.fork();
                const workerRecords = (i === numCPUs - 1) ? numRecords - totalRecordsWritten : recordsPerWorker;
                worker.send({ structureMap: Array.from(structureMap), recordsToGenerate: workerRecords, workerId: i });
                totalRecordsWritten += workerRecords;

                // Handle messages from workers
                worker.on('message', (message: string) => {
                    if (message === 'completed') {
                        if (++completedWorkers === numCPUs) {
                            outputStream.end(() => {
                                const endTime = performance.now();
                                console.log(`Total execution time: ${(endTime - startTime).toFixed(2)} milliseconds`);
                                process.exit(0);
                            });
                        }
                    } else {
                        outputStream.write(message);
                    }
                });
            }

            // Handle worker exits
            cluster.on('exit', (worker) => {
                console.log(`Worker ${worker.process.pid} died`);
            });
        })
        .catch(error => {
            console.error("Error processing data:", error);
        });
} else {
    // Worker process
    process.on('message', (message: { structureMap: [string, ColumnInfo][], recordsToGenerate: number, workerId: number }) => {
        const { structureMap, recordsToGenerate } = message;
        const structureMapObj = new Map(structureMap);
        
        // Create a transform stream for the worker
        const workerStream = new Transform({
            objectMode: true,
            transform(chunk, encoding, callback) {
                if (process.send) {
                    process.send(chunk);
                }
                callback();
            }
        });

        // Generate and stream data
        generateAndStreamData(structureMapObj, recordsToGenerate, workerStream)
            .then(() => {
                if (process.send) {
                    process.send('completed');
                }
            })
            .catch(error => {
                console.error(`Worker error:`, error);
                process.exit(1);
            });
    });
}