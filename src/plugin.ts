import * as aws from 'aws-sdk';

export default class CopyStackTablesPlugin {

    public commands: {};
    public hooks: {};
    private readonly HASH: string = 'HASH';
    private readonly RANGE: string = 'RANGE';
    private targetStage!: string;
    private sourceStage!: string;
    private overwriteAllData: boolean = false;

    constructor(private serverless: Serverless, private options: Serverless.Options) {

        this.serverless.variables.copyData = {};
        this.serverless.variables.copyKeys = {};

        this.commands = {
            'copy-stack-tables': {
                lifecycleEvents: [
                    'preCopy',
                    'validateTables',
                    'downloadTables',
                    'clearTables',
                    'uploadTables'
                ],
                options: {
                    'source-stage': {
                        required: true,
                        usage: 'Stage you want to copy data from'
                    },
                    'target-stage': {
                        required: true,
                        usage: 'Stage you want to copy data to'
                    },
                    'overwrite-all-data': {
                        required: false,
                        usage: 'Overwrite or update items in table'
                    }
                },
                usage: 'Pushes data from one database to another'
            }
        };

        this.hooks = {
            'copy-stack-tables:preCopy': this.preCopy.bind(this),
            'copy-stack-tables:validateTables': this.validateAllTables.bind(this),
            'copy-stack-tables:downloadTables': this.downloadAllTables.bind(this),
            'copy-stack-tables:clearTables': this.clearTables.bind(this),
            'copy-stack-tables:uploadTables': this.uploadTables.bind(this),
            'after:deploy:deploy': this.copyAfterDeploy.bind(this)
        };
    }

    private get allValuesProvided(): boolean {
        const copyDataDeploy = this.serverless.service.custom.copyDataDeploy;
        const doCopy: boolean = copyDataDeploy.targetStage &&
        copyDataDeploy.sourceStage && copyDataDeploy.targetStage === this.serverless.getProvider('aws').getStage() ? true : false;

        return doCopy;
    }

    private async copyAfterDeploy() {

        if (this.allValuesProvided) {

            this.targetStage = this.serverless.service.custom.copyDataDeploy.targetStage;
            this.sourceStage = this.serverless.service.custom.copyDataDeploy.sourceStage;
            this.overwriteAllData = this.serverless.service.custom.copyDataDeploy.overwriteAllData ? this.serverless.service.custom.copyDataDeploy.overwriteAllData : false;

            await this.validateAllTables();
            await this.downloadAllTables();
            await this.clearTables();
            await this.uploadTables();

        }
    }

    private preCopy() {

        this.targetStage = this.options['target-stage'];
        this.sourceStage = this.options['source-stage'];
        this.overwriteAllData = this.options['overwrite-all-data'] ? this.options['overwrite-all-data'] : false;
    }

    private async validateAllTables() {

        await this.validateTables();
        await this.validateTables(true);
    }

    private async downloadAllTables() {

        await this.downloadTables();
        await this.downloadTables(true);

    }

    private async validateTables(isTarget = true) {

        await Promise.all(this.getCurrentStageTableNames().map(async (tableName) => {

            const data: any = await this.describePromise(this.getDynamoDB(), {TableName: this.getTableStageName(tableName, isTarget)});

            const keys: any = {};
            keys[this.HASH] = data.Table.KeySchema.find((obj: any) => obj.KeyType === this.HASH).AttributeName;
            if (data.Table.KeySchema.find((obj: any) => obj.KeyType === this.RANGE)) {
                keys[this.RANGE] = data.Table.KeySchema.find((obj: any) => obj.KeyType === this.RANGE).AttributeName;
            }

            this.serverless.variables.copyKeys[this.getTableStageName(tableName, isTarget)] = keys;

        }));

    }

    private async clearTables(isTarget = true) {

        if (this.overwriteAllData) {

            await Promise.all(this.getCurrentStageTableNames().map(async (tableName) => {

                await this.deleteItems(this.getTableStageName(tableName, isTarget), this.serverless.variables.copyKeys[this.getTableStageName(tableName, isTarget)]);

            }));
        }
    }

    private async downloadTables(isTarget = false) {

        await Promise.all(this.getCurrentStageTableNames(isTarget).map(async (tableName) => {

            await this.downloadItems(tableName, isTarget);

        }));

    }

    private async uploadTables() {

        await Promise.all(this.getCurrentStageTableNames().map(async (tableName) => {

            await this.uploadData(tableName);

        }));

    }

    private async deleteItems(tableName: string, keys: any) {

        const toBeRemoved: any[] = [];

        this.serverless.variables.copyData[tableName].Items.forEach((data: any) => {

            const params = {
                TableName: tableName,
                Key: {}
            };

            params.Key[keys.HASH] = data[keys.HASH];
            if (keys.RANGE) {
                params.Key[keys.RANGE] = data[keys.RANGE];
            }

            toBeRemoved.push(this.deletePromise(this.getDynamoDB(), params));
        });

        await Promise.all(toBeRemoved).then(() => {
            this.serverless.cli.log(`Deleted ${JSON.stringify(toBeRemoved.length)} items from ${tableName}`);
        }).catch((error) => {
            this.serverless.cli.log(`Data delete failed: ${JSON.stringify(error)}`);
        });

    }

    private downloadItems(tableName: string, isTarget = false) {

        return new Promise((resolve, reject) => {

            const dynamodb = this.getDynamoDB();
            const params = {
                TableName: this.getTableStageName(tableName, isTarget)
            };

            dynamodb.scan(params, (error, result) => {
                if (error) {
                    this.serverless.cli.log(`Error on downloading data from ${this.getTableStageName(tableName, isTarget)} : ${JSON.stringify(error)}`);
                    return reject(error);
                }

                this.serverless.variables.copyData[this.getTableStageName(tableName, isTarget)] = result;
                if (!isTarget) {
                    const numItems = this.serverless.variables.copyData[this.getTableStageName(tableName, isTarget)].Items.length;
                    this.serverless.cli.log(`Downloaded ${numItems} items from ${this.getTableStageName(tableName, isTarget)}`);
                }
                return resolve(result);
            });
        });

    }

    private async uploadData(tableName: string) {

        const dynamodb = this.getDynamoDB();
        const uploads: any[] = [];

        this.serverless.variables.copyData[this.getTableStageName(tableName, false)].Items.forEach((data: any) => {
            const params = {
                TableName: this.getTableStageName(tableName, true),
                Item: data
            };
            uploads.push(this.putPromise(dynamodb, params));
        });

        await Promise.all(uploads).then(() => {
            this.serverless.cli.log(`Uploaded ${JSON.stringify(uploads.length)} items to ${this.getTableStageName(tableName, true)}`);
        });
    }

    private describePromise(dynamodb: aws.DynamoDB, params: any) {

        return new Promise((resolve, reject) => {
            dynamodb.describeTable(params, (error, data) => {
                if (error) {
                    this.serverless.cli.log(`Error on downloading data from ${params.TableName} : ${JSON.stringify(error)}`);

                    return reject(error);
                }
                return resolve(data);
            });
        });
    }

    private deletePromise(dynamodb: aws.DynamoDB, params: any) {

        return new Promise((resolve, reject) => {
            dynamodb.deleteItem(params, (error, data) => {
                if (error) {
                    return reject(error);
                }
                return resolve(data);
            });
        });
    }

    private putPromise(dynamodb: aws.DynamoDB, params: any) {

        return new Promise((resolve, reject) => {
            dynamodb.putItem(params, (error) => {
                if (error) {
                    return reject(error);
                }
                return resolve();
            });
        });
    }

    private getCurrentStageTableNames(isTarget = false) {

        const tableNames = [];

        for (const resource in this.serverless.service.resources.Resources) {

            if (this.serverless.service.resources.Resources[resource].Type === 'AWS::DynamoDB::Table') {

                tableNames.push(this.serverless.service.resources.Resources[resource].Properties.TableName);

            }
        }

        return tableNames;

    }

    private getTableStageName(tableName: string, isTarget = false) {

        return isTarget ? tableName.replace(this.serverless.service.custom.stage, this.targetStage) : tableName.replace(this.serverless.service.custom.stage, this.sourceStage);

    }

    private getDynamoDB() {
        aws.config.update({
            region: this.serverless.service.provider.region,
            apiVersions: {
                dynamodb: '2012-08-10'
            }
        });

        return new aws.DynamoDB();
    }

}
