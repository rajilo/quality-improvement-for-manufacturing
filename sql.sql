CREATE TABLE Manufacturing_Process (
  MachineID TEXT PRIMARY KEY,
  ProductionBatchNumber TEXT,
  OperatorID TEXT,
  Start_End_Time DATETIME,
  Defect_Rate DECIMAL
);

CREATE TABLE Product (
  ProductID TEXT PRIMARY KEY,
  ProductionBatchNumber TEXT UNIQUE,
  ProductName TEXT,
  Defect_Rate DECIMAL
);

CREATE TABLE Supplier (
  SupplierID TEXT PRIMARY KEY,
  MaterialType TEXT,
  DeliveryDate DATE,
  BatchNumber TEXT
);

CREATE TABLE Quality_Metric (
  QCBatchID TEXT PRIMARY KEY,
  DefectType TEXT,
  InspectionDate DATE,
  BatchNumber TEXT,
  ActionTaken_ScrapOrRework TEXT
);


