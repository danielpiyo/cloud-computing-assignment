CREATE TABLE CustomerPurchases (
    CustomerID INTEGER NOT NULL,    -- Customer identifier
    PurchaseDate DATE NOT NULL,     -- Date of purchase
    ProductID INTEGER,              -- Product identifier
    Quantity INTEGER,               -- Quantity purchased
    Price DOUBLE,                   -- Price of the product
    PRIMARY KEY (CustomerID, PurchaseDate)
);