use lozon;
go
set nocount, xact_abort on;
go

declare @carriage table (carriageid dbo.bident)
insert into @carriage


select 24497895003000						-- ID перевозки


 
 -- полный состав перевозки
;with  articles as ( 
	select oa.objecttypeid as articletypeid
		 , aic.articleid 
		 , aic.carriageid as container
		 , aic.momentout
		 , c.carriageid as carriage
		 , 0 as lvl
	from articleincarriage aic
	inner join @carriage c on c.carriageid=aic.carriageid
	inner join object oa  on oa.id=aic.articleid and not oa.objecttypeid = 2653000
	union all 
	select oa.objecttypeid as articletypeid
		 , ca.articleid as articleid
		 , ca.containerid as container
		 , ca.momentout
		 , carriage
		 , a.lvl + 1 as lvl
	from  articles a
	inner join dbo.containerarticle ca on ca.containerid = a.articleid  
		and ( ca.momentin <= a.momentout or a.momentout is null)  
		and ( ca.momentout >= a.momentout or ca.momentout is null) 
	inner join object oc on oc.id=ca.containerid and not oc.objecttypeid = 2659000
	inner join object oa on oa.id=ca.articleid)  
 
select * from articles
