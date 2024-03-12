use lozon;
go
set nocount, xact_abort on;
go

declare
  @postingid			dbo.bident
, @exemplarid			dbo.bident
, @message				dbo.string
, @info					dbo.string
, @placeidexemplar		dbo.bident
, @placeidposting		dbo.bident
, @appuserid			dbo.bident					= dbo.appusergetcurrent()
, @batchsize			int							= 50						-- Размер батча
, @totalcount			int
, @batchcount			int							= 0
, @sdnumber				dbo.string					= 'SD911-4593836'  
, @idlist				dbo.uniqueidlistoptimized;

drop table if exists #res;
create table #res (message dbo.string, id dbo.bident);

drop table if exists #posting;
create table #posting (id dbo.bident);

with container as (
select ca.containerid
from dbo.containerarticle ca
join dbo.object o on o.id = ca.ArticleID 
where o.stateid in (108249583000,3515020128000)						-- Статус экза "Списан", "Возвращен принципалу"
and exists (select 1 
			from dbo.objectdirectory od
			where ca.containerid = od.objectid
			and od.directoryid = 9289853234000))					-- Хар-ка "Возвратное отправление"

insert into #posting (id)

select o.id
from dbo.object o 
where o.stateid in (44070000,235772000,44079000)					-- Статус постинга "Сформирован", "Недостача", "Прибыл в место назначения"
and o.id = some(select c.containerid from container c) 

set @totalcount = (select count(*) from #posting)

while 1 = 1
begin

delete from @idlist;

with cte as(
	select top(@batchsize) id 
	from #posting p)

delete from cte
output deleted.id into @idlist

	if @@rowcount = 0
    begin 
      break;
    end;

set @batchcount = @batchcount + (select count(*) from @idlist t)


declare my_cur cursor fast_forward for
 
select ca.containerid
, ca.articleid
from @idlist t
join dbo.containerarticle ca on ca.containerid = t.id 

 open my_cur;
 fetch next from my_cur 
 into  @postingid
	 , @exemplarid;

 while  @@fetch_status = 0 
 begin
    begin try
      begin tran

-- Если постинг уже расформирован
if exists (select 1 
		   from dbo.object o 
		   where o.id = @PostingID and o.stateid = 44076000	   				
		  )

	begin
		
		exec dbo.abort
		  @Code = @@PROCID 
		, @Message = 'Постинг уже расформирован'

	end

-- если экземпляр в статусе released
if exists (select 1
		   from dbo.object o 
		   join dbo.containerarticle ca on ca.containerid = @postingid
		   where o.id = @exemplarid and o.stateid = 108249583000
		   )

	begin
	
		exec wms.articlecheckoutcontainerandcarriage
		  @articleid = @exemplarid

		exec support.objectsetstate       
		  @id           = @postingid   
		, @statesysname = 'banded'  
		, @sdnumber     = @sdnumber;

		exec support.objectsetstate       
		  @id           = @postingid   
		, @statesysname = 'disbanding'  
		, @sdnumber     = @sdnumber;

		exec support.objectsetstate       
		  @id           = @postingid   
		, @statesysname = 'disbanded'  
		, @sdnumber     = @sdnumber;


		set @message = 'Экземпляр в статусе возвращен принципалу. Постинг расфомирован'
	
	end


-- если экз в статусе writtenoff
if exists (select 1
		   from dbo.object o 
		   join dbo.containerarticle ca on ca.containerid = @postingid
		   where o.id = @exemplarid and o.stateid = 3515020128000
		   )

	begin
	
		exec wms.articlecheckoutcontainerandcarriage
		  @articleid = @exemplarid

		exec support.articlecurrentplaceclear
		  @id = @exemplarid
		, @statetransitid = 3515021082000

		exec support.objectsetstate       
		  @id           = @postingid   
		, @statesysname = 'banded'  
		, @sdnumber     = @sdnumber;

		exec support.objectsetstate       
		  @id           = @postingid   
		, @statesysname = 'disbanding'  
		, @sdnumber     = @sdnumber;

		exec support.objectsetstate       
		  @id           = @postingid   
		, @statesysname = 'disbanded'  
		, @sdnumber     = @sdnumber;

		set @message = 'Экземпляр в статусе списан. Постинг расфомирован'
	
	end

exec support.eventadd
  @id =    null
, @ownerobjectid =  @postingid
, @message = 'Корректировка RP-постингов'
, @sdnumber = @sdnumber

   
    insert into #res (message, id)
    values (@message, @postingid)

      commit tran
    end try

    begin catch
​
  insert into #res (message, id)
  values (error_message(), @postingid);
​
    end catch

 fetch next from my_cur 
 into  @postingid
	 , @exemplarid;
    
 end
 close my_cur;
 deallocate my_cur;

 set @info = concat('Обработано ', @batchcount, ' из ', @totalcount);

 print @info 

 end;


select r.message
, ca.containerid
, ca.articleid
from #res r
join dbo.containerarticle ca on ca.containerid = r.id 
