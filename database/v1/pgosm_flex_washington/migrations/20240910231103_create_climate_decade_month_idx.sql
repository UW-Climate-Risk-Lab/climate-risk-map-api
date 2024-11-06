-- Create index on decade_month, will be common query
CREATE INDEX idx_scenariomip_on_decade_month ON climate.scenariomip (decade, month);