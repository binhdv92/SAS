( ( TreatAs(_Number_, DatePart(Now())) - TreatAs(_Number_,
DateFromMDY(1, 1, 2015)) ) + 1 ) - ( TreatAs(_Number_, 'DATE'n) -
TreatAs(_Number_, DateFromMDY(1, 1, 2015)) )
