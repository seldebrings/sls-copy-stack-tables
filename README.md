## Serverless Copy Stack Tables

Serverless framework plugin for copying all dynamoDB tables in AWS stack from one stage to another.
Currently only works within the same region.
Table names must use the standard naming convention:

```yml
  TableName: mytablename-${opt:stage, self:provider.stage} # ex mytablename-dev
  ```

#### Install

```bash
$  npm install serverless-copy-stack-tables
```

#### Usage in command prompt

```bash
$  sls copy-stack-tables --target-stage dev --source-stage prod --overwrite-all-data false
```

#### Options

```bash
--target-stage            Stage to copy to. Required
--source-stage            Stage to copy from. Required
--overwrite-all-data      Overwrite all items in table or update, default is false. Optional
```

#### Usage on deployment

```yml
custom:
  ..
  # Copy all databases from prod stage when deploying to the staging stage
  copyDataDeploy:
    targetStage: staging    #Required
    sourceStage: prod       #Required
    overwriteAllData: true  #Optional, default is false
  ..
  ```
