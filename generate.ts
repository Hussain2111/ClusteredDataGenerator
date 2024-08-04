import { faker } from '@faker-js/faker';
import * as fs from 'fs';
import cluster from 'cluster';
import os from 'os';
import { performance } from 'perf_hooks';
import { Transform } from 'stream';

const startTime = performance.now();
const BATCH_SIZE = 10000;
const STRING_POOL_SIZE = 10000;

type ColumnInfo = {
    DataType: string;
    Format?: string;
    Order: string;
    ExcelColumnName: string;
    IsDate: string;
    IsDouble: string;
    IsInteger: string;
};


const stringPool = Array.from({ length: STRING_POOL_SIZE }, () => faker.string.alpha({ length: { min: 1, max: 50 } }));
let stringPoolIndex = 0;

function getNextString(): string {
    const value = stringPool[stringPoolIndex];
    stringPoolIndex = (stringPoolIndex + 1) % STRING_POOL_SIZE;
    return value;
}

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

const generateAndStreamData = (structureMap: Map<string, ColumnInfo>, recordsToGenerate: number, outputStream: Transform): Promise<void> => {
    return new Promise((resolve) => {
        let recordsWritten = 0;
        let batch: string[] = [];

        function writeBatch() {
            if (batch.length > 0) {
                outputStream.write(batch.join('\n') + '\n');
                batch = [];
            }
        }

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

if (cluster.isPrimary) {
    const numCPUs = os.cpus().length;
    const numRecords = parseInt(process.argv[2]) || 1000;
    const recordsPerWorker = Math.ceil(numRecords / numCPUs);

    console.log(`Generating ${numRecords} records using ${numCPUs} workers`);

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

            for (let i = 0; i < numCPUs; i++) {
                const worker = cluster.fork();
                const workerRecords = (i === numCPUs - 1) ? numRecords - totalRecordsWritten : recordsPerWorker;
                worker.send({ structureMap: Array.from(structureMap), recordsToGenerate: workerRecords, workerId: i });
                totalRecordsWritten += workerRecords;

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

            cluster.on('exit', (worker) => {
                console.log(`Worker ${worker.process.pid} died`);
            });
        })
        .catch(error => {
            console.error("Error processing data:", error);
        });
} else {
    process.on('message', (message: { structureMap: [string, ColumnInfo][], recordsToGenerate: number, workerId: number }) => {
        const { structureMap, recordsToGenerate } = message;
        const structureMapObj = new Map(structureMap);
        
        const workerStream = new Transform({
            objectMode: true,
            transform(chunk, encoding, callback) {
                if (process.send) {
                    process.send(chunk);
                }
                callback();
            }
        });

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
