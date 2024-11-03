CREATE TABLE [Manufacturing_Process] (
  [MachineID] TEXT(PK),
  [ProductionBatchNumber] TEXT,
  [OperatorID] text,
  [Start_End_Time] DATETIME,
  [Defect_Rate] DECIMAL
)

CREATE TABLE [Product] (
  [ProductID] TEXT(PK),
  [ProductionBatchNumber] TEXT(UNIQUE),
  [ProductName] TEXT,
  [Defect_Rate] DECIMAL
)

CREATE TABLE [Supplier] (
  [SupplierID] TEXT(PK),
  [MaterialType] TEXT,
  [DeliveryDate] DATE,
  [BatchNumber] TEXT
)

CREATE TABLE [Quality_Metric] (
  [QCBatchID] TEXT(PK),
  [DefectType] text,
  [InspectionDate] DATE,
  [BatchNumber] TEXT,
  [ActionTaken_ScrapOrRework] TEXT
)



