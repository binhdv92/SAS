( ( TreatAs(Number, DatePart(Now())) - TreatAs(Number,
DateFromMDY(1, 1, 2015)) ) + 1 ) - ( TreatAs(Number, 'DATE'n) -
TreatAs(Number, DateFromMDY(1, 1, 2015)) )
