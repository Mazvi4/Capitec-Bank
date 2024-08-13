
/*
 * DAR-4863 Excel Tools  APS Migration  BDC-277 AML Portfolio of Evidence
 * 
 * Author	: Wynand van Staden
 * Date		: 2024-07-15
 * 
 */

with client as ( 
	select
		da.clientnumber as clientnumber
	from edw_core_base.dimaccount as da
	where da.accountnumberwithcheckdigit = '1503098492'   -- filter by account
	group by da.clientnumber		
),
pre_account as (
	select 
		da.clientnumber 
		,fd.currentbalanceamount
		,fd.accountnumber
		,da.accountnumberwithcheckdigit
		,da.accountstatusname
		,fd.productkey
		,dp.productgroupname
		,dp.productname
		,da.accountopendate
		,db.branchcode 
		,db.branchname 
		,row_number() over(partition by fd.accountnumber, da.accountnumberwithcheckdigit order by fd.coveragedatekey desc) as row_num
	from edw_core_base.dimaccount as da
	inner join edw_core_base.factdepositaccountdailycoverage as fd on fd.accountnumber = da.accountnumber
	left join edw_core_base.dimproduct as dp on dp.productkey = fd.productkey
	inner join edw_core_base.dimbranch as db on db.branchkey = fd.homebranchkey
	where da.rowiscurrent = 1 
	and da.clientnumber = (select clientnumber from client)
	group by
		da.clientnumber 
		,fd.currentbalanceamount
		,fd.accountnumber
		,da.accountnumberwithcheckdigit
		,da.accountstatusname
		,fd.productkey
		,dp.productgroupname
		,dp.productname
		,da.accountopendate
		,db.branchcode 
		,db.branchname
		,fd.coveragedatekey
	union all 
	select 
		da.clientnumber 
		,coalesce(fd.currentbalanceamount, 0) as currentbalanceamount
		,fl.accountnumber
		,da.accountnumberwithcheckdigit
		,da.accountstatusname
		,fl.productkey
		,dp.productgroupname
		,dp.productname
		,da.accountopendate
		,db.branchcode 
		,db.branchname 
		,row_number() over(partition by fl.accountnumber, da.accountnumberwithcheckdigit order by fl.coveragedatekey desc) as row_num
	from edw_core_base.dimaccount as da
	inner join edw_core_base.factloanaccountdailycoverage as fl on fl.accountnumber = da.accountnumber
	left join edw_core_base.factdepositaccountdailycoverage AS fd ON fd.accountnumber = fl.accountnumber 
		AND (fd.coveragedatekey = (SELECT MAX(coveragedatekey) FROM edw_core_base.factdepositaccountdailycoverage) 
		OR fd.isfinalload = 1)
	left join edw_core_base.dimproduct as dp on dp.productkey = fl.productkey
	inner join edw_core_base.dimbranch as db on db.branchkey = fl.originatingbranchkey
	where da.rowiscurrent = 1 
	and da.clientnumber = (select clientnumber from client)
	group by
		da.clientnumber 
		,fd.currentbalanceamount
		,fl.accountnumber
		,da.accountnumberwithcheckdigit
		,da.accountstatusname
		,fl.productkey
		,dp.productgroupname
		,dp.productname
		,da.accountopendate
		,db.branchcode 
		,db.branchname
		,fl.coveragedatekey
),
account as (
	select 
		clientnumber 
		,currentbalanceamount
		,accountnumber
		,accountnumberwithcheckdigit
		,accountstatusname
		,productkey
		,productgroupname
		,productname
		,accountopendate
		,branchcode 
		,branchname 
	from pre_account as pa
	where pa.row_num = 1
),
card as (
	select 
		c.cardholdertypecode
		,c.cardnumber
		,c.primarycardnumber
		,cardstatusname
		,c.cardholdertypename
		,c.embossedname 
		,c.partyidentifiernumber
		,cardcreationdate
		,LEFT(c.clientnumber, 8) as card_clientnumber
		,max(c.cardkey) as cardkey 
	from edw_core_base.dimcard as c
	where LEFT(c.clientnumber, 8) in (select clientnumber from account)
	and c.cardholdertypecode = 'P'
	and rowiscurrent = 1
	group by 
		clientnumber 
		,c.cardholdertypecode
		,c.cardnumber
		,c.primarycardnumber
		,cardstatusname
		,c.cardholdertypename
		,c.embossedname 
		,c.partyidentifiernumber
		,cardcreationdate
),
prep_pre_client_details as (  
	select *
	from (
		select 
			cifnumber
			,residentialaddresskey
			,postaladdresskey
			,employerkey
			,fc.primarydepositaccountnumber
			,fc.clientsnapshottimestamp 
			,row_number() over(partition by fc.cifnumber order by fc.clientsnapshottimestamp desc) as rn
		from edw_core_base.factclient as fc
		where cifnumber in (select card_clientnumber from card)
	) where rn = 1	
),
pre_client_details as (  
	select 
		cifnumber
		,residentialaddresskey
		,postaladdresskey
		,accountnumberwithcheckdigit
		,de.employername
	from prep_pre_client_details as fc
	inner join edw_core_base.dimaccount as da on da.accountnumber = fc.primarydepositaccountnumber
	inner join edw_core_base.dimemployer as de on de.employerkey = fc.employerkey
	group by
		cifnumber
		,residentialaddresskey
		,postaladdresskey
		,accountnumberwithcheckdigit
		,de.employername
),
client_details as (
	select
		fc.cifnumber
		,case when (dad.addressline1text = dad2.addressline1text) 
			and len(dad.addressline2text) = 0 
			and len(dad.addressline3text) = 0 
			then (dad2.addressline1text + ', ' + dad2.addresscityname)
		else (dad.addressline1text + ', ' + dad.addressline2text + ', ' + dad.addressline3text + ', ' 
			+ dad.addresscityname + ', ' + dad.addresspostcode) 
		end as home_address
		,accountnumberwithcheckdigit
		,employername
	from pre_client_details as fc
	inner join edw_core_base.dimaddress as dad on dad.addresskey = fc.residentialaddresskey
	inner join edw_core_base.dimaddress as dad2 on dad2.addresskey = fc.postaladdresskey
	group by 
		fc.cifnumber 
		,home_address 
		,accountnumberwithcheckdigit
		,employername
)
select 
	a.accountnumberwithcheckdigit as account_number_check_digit
	,dc.cifwithcheckdigit as cif_number
	,a.productgroupname as product_group
	,a.productname as product_desc
	,dc.titlename as title
	,dc.firstname as first_name
	,dc.lastname as last_name
	,dc.birthdate as date_of_birth
	,(datediff(day, dc.birthdate, current_date) / 365) as age
	,dc.clientcreatedate as first_client_contact_date
	,a.accountopendate as date_opened
	,a.branchcode as home_branch_code
	,a.branchname as home_branch_name
	,c.primarycardnumber as card_prim_no
	,c.cardstatusname as card_status
	,c.cardholdertypename as card_type
	,c.embossedname as card_name
	,c.cardcreationdate as card_allocated_date
	,dc.partyidentifiertypenumber as id_number
	,dc.partyidentifiertypename as id_type
	,cd.employername as employer_name
	,cd.home_address
	,dc.homephonenumber as phone_home
	,dc.workphonenumber as phone_work
	,dc.mobilephonenumber as phone_cell
	,a.currentbalanceamount as balance_on_account
	,a.accountstatusname as account_status
from edw_core_base.dimclient as dc
inner join card as c on c.card_clientnumber = dc.cifnumber
inner join account as a on a.clientnumber = c.card_clientnumber
inner join client_details as cd on cd.cifnumber = dc.cifnumber
where dc.cifnumber in (select clientnumber from client)
group by
	a.accountnumberwithcheckdigit 
	,dc.cifwithcheckdigit
	,a.productgroupname
	,a.productname
	,dc.titlename
	,dc.firstname
	,dc.lastname
	,dc.birthdate
	,age
	,dc.clientcreatedate
	,a.accountopendate
	,a.branchname
	,a.branchcode
	,c.primarycardnumber
	,c.cardstatusname
	,c.cardholdertypename
	,c.embossedname 
	,c.cardcreationdate
	,dc.partyidentifiertypenumber
	,dc.partyidentifiertypename
	,cd.employername
	,cd.home_address
	,dc.homephonenumber
	,dc.workphonenumber
	,dc.mobilephonenumber
	,a.currentbalanceamount 
	,a.accountstatusname ;



