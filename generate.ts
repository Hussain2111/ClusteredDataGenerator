import { faker } from '@faker-js/faker';
import * as fs from 'fs';
import cluster from 'cluster';
import os from 'os';
import { performance } from 'perf_hooks'; // Import performance module
import path from 'path';

const startTime = performance.now();
// Define the structure of each column in the JSON configuration
type ColumnInfo = {
    DataType: string;
    Format?: string;
    Order: string;
    ExcelColumnName: string;
    IsDate: string;
    IsDouble: string;
    IsInteger: string;
};

// Define the overall structure of the JSON configuration
type JsonStructure = ColumnInfo[];

/**
 * Checks for duplicate order numbers in the JSON structure
 * @param structure The JSON structure to check
 * @throws Error if a duplicate order number is found
 */
const checkDuplicateOrders = (structure: JsonStructure): void => {
    const orderNumbers = new Set<string>();
    structure.forEach(column => {
        if (orderNumbers.has(column.Order)) {
            throw new Error(`Duplicate order number found: ${column.Order}`);
        }
        orderNumbers.add(column.Order);
    });
};

/**
 * Generates a random value based on the column configuration
 * @param column The column configuration
 * @returns A random value of the appropriate type
 */
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

const createPersonObject = (structure: JsonStructure): string => {
    return structure.map(column => {
        const value = generateRandomValue(column);
        return typeof value === 'string' && value.includes(',') ? `"${value}"` : value;
    }).join(',');
};

const writeCSVInBatches = (structure: JsonStructure, recordsToGenerate: number, filePath: string, batchSize: number = 50000) => {
    return new Promise<void>((resolve, reject) => {
        const writeStream = fs.createWriteStream(filePath, { highWaterMark: 64 * 1024 });
        const headers = structure.map(col => col.ExcelColumnName).join(',') + '\n';
        writeStream.write(headers);

        let recordsWritten = 0;

        const writeBatch = () => {
            let batch = '';
            const batchEnd = Math.min(recordsWritten + batchSize, recordsToGenerate);

            for (; recordsWritten < batchEnd; recordsWritten++) {
                batch += createPersonObject(structure) + '\n';
            }

            if (!writeStream.write(batch)) {
                writeStream.once('drain', () => {
                    if (recordsWritten < recordsToGenerate) {
                        setImmediate(writeBatch);
                    } else {
                        writeStream.end(() => resolve());
                    }
                });
            } else if (recordsWritten < recordsToGenerate) {
                setImmediate(writeBatch);
            } else {
                writeStream.end(() => resolve());
            }
        };

        writeBatch();

        writeStream.on('error', reject);
    });
};



if (cluster.isPrimary) {
    const numCPUs = os.cpus().length;
    const numRecords = parseInt(process.argv[2]) || 1000;
    const recordsPerWorker = Math.ceil(numRecords / numCPUs);

    console.log(`Generating ${numRecords} records using ${numCPUs} workers`);

    fs.promises.readFile('input.json', 'utf8')
        .then(data => {
            const jsonStructure: ColumnInfo[] = JSON.parse(data);
            checkDuplicateOrders(jsonStructure);
            jsonStructure.sort((a, b) => parseInt(a.Order) - parseInt(b.Order));

            let completedWorkers = 0;

            for (let i = 0; i < numCPUs; i++) {
                const worker = cluster.fork();
                worker.send({ jsonStructure, recordsToGenerate: recordsPerWorker, workerId: i });

                worker.on('message', (message: { completed: boolean }) => {
                    if (message.completed && ++completedWorkers === numCPUs) {
                        console.log('All workers completed. Merging files...');
                        mergeFiles(numCPUs)
                            .then(() => {
                                const endTime = performance.now();
                                console.log(`Total execution time: ${(endTime - startTime).toFixed(2)} milliseconds`);
                                process.exit(0);
                            })
                            .catch(error => {
                                console.error("Error merging files:", error);
                                process.exit(1);
                            });
                    }
                });
            }

            cluster.on('exit', (worker) => {
                console.log(`Worker ${worker.process.pid} died`);
            });
        })
        .catch(error => {
            console.error("Error processing data:", error);
        });
} else {
    process.on('message', (message: { jsonStructure: JsonStructure, recordsToGenerate: number, workerId: number }) => {
        const { jsonStructure, recordsToGenerate, workerId } = message;
        const workerFilePath = path.join(__dirname, `output_part_${workerId}.csv`);
        
        writeCSVInBatches(jsonStructure, recordsToGenerate, workerFilePath)
            .then(() => {
                if (process.send) {
                    process.send({ completed: true });
                }
            })
            .catch(error => {
                console.error(`Worker ${workerId} error:`, error);
                process.exit(1);
            });
    });
}

function mergeFiles(numFiles: number): Promise<void> {
    return new Promise((resolve, reject) => {
        const outputStream = fs.createWriteStream('output.csv', { highWaterMark: 64 * 1024 });
        let currentFile = 0;

        const appendNextFile = () => {
            if (currentFile >= numFiles) {
                outputStream.end();
                return;
            }

            const filePath = path.join(__dirname, `output_part_${currentFile}.csv`);
            const readStream = fs.createReadStream(filePath, { highWaterMark: 64 * 1024 });

            readStream.pipe(outputStream, { end: false });

            readStream.on('end', () => {
                fs.unlink(filePath, (err) => {
                    if (err) console.error(`Error deleting file ${filePath}:`, err);
                });
                currentFile++;
                appendNextFile();
            });

            readStream.on('error', (error) => {
                console.error(`Error reading file ${filePath}:`, error);
                reject(error);
            });
        };

        outputStream.on('finish', () => {
            console.log('All files have been merged successfully');
            resolve();
        });

        outputStream.on('error', (error) => {
            console.error('Error writing to output file:', error);
            reject(error);
        });

        appendNextFile();
    });
}