use metazon;
go
set nocount, xact_abort on;
go


declare
  @ObjectID				dbo.bident
, @Warehouse			dbo.string = '%МСК_СТРОГИНО_ХАБ%'                       -- Наименование склада
, @SourceKey			dbo.bident = 1020000842858000							-- sourcekey lozon из файла
, @DirectoryID1			dbo.bident = 11154937256040								-- характеристика Признак WMS склада
, @DirectoryID2			dbo.bident = 11175546362370								-- характеристика Склады внешних систем
, @DirectoryID3			dbo.bident = 11299354568140								-- характеристика WMS
, @ObjectTypeSysName1   dbo.string = 'ExtValueActualAddressForCheckEDOXmlUTD'   -- значение Адрес объекта для проверки во входящих ЭДО XML УПД
, @ObjectTypeSysName2   dbo.string = 'ExtValueActualDetailsForMakeUTD'			-- значение Адрес объекта и реквизиты для создания УПД
, @SDnumber				dbo.string = 'SD911-3904803'							-- Номер заявки




drop table if exists #res
create table #res (Message dbo.string, ID dbo.bident)

begin
   begin try
     begin tran

	  select top 1 *
	  from dbo.personlst pl
	  where pl.name like @Warehouse					 
	  and pl.ObjectTypeID = 20103621000			     -- тип "Склад хранения"
	  
	  if @@rowcount = 0
	  	begin 
	  		exec dbo.LogisticExecProcLocationConfigureByRezon
	  	end
	  
	  set @ObjectID = (select pl.id	from dbo.personlst pl where pl.name like @Warehouse and pl.ObjectTypeID = 20103621000)

	  if not exists (select 1 
					 from dbo.objectsourcelst os
					 where os.ObjectID = @ObjectID 
					 and os.srcSysName = 'Clearing'
					)
	  	begin
	  
	  		exec dbo.ObjectSourceAdd
	  		  @ObjectID = @ObjectID
	  		, @SourceSysName = 'Clearing'
	  		, @TableName ='PlaceStore'
	  		, @SourceKey = @SourceKey

		end

	  if not exists (select 1
	  				 from dbo.objectdirectory od
	  				 where od.ObjectID = @ObjectID 
	  				   and od.DirectoryID = @DirectoryID1
	  				)
		begin 

	  		exec dbo.objectdirectoryadd
	  		  @DirectoryID = @DirectoryID1   
	  		, @ObjectID = @ObjectID

		end

	  if not exists (select 1
	   				 from dbo.objectdirectory od
	   				 where od.ObjectID = @ObjectID 
	   				   and od.DirectoryID = @DirectoryID2
	   				)
		begin 

	  		exec dbo.objectdirectoryadd
	  		  @DirectoryID = @DirectoryID2
	  		, @ObjectID = @ObjectID

		end

	  if not exists (select 1
	   				 from dbo.objectdirectory od
	   				 where od.ObjectID = @ObjectID 
	   				   and od.DirectoryID = @DirectoryID3
	   				)
		begin 

	  		exec dbo.objectdirectoryadd
	  		  @DirectoryID = @DirectoryID3
	  		, @ObjectID = @ObjectID
		
		end

	  if not exists (select 1
				     from dbo.extvaluelst ev
				     where ev.OwnerObjectID = @ObjectID
				     and ev.ObjectTypeSysName = @ObjectTypeSysName1
				    )
		begin

	  		exec dbo.extvalueadd
	  		  @ID = null
	  		, @ObjectTypeSysName = @ObjectTypeSysName1
	  		, @OwnerObjectID = @ObjectID

		end

	  if not exists (select 1 
				     from dbo.extvaluelst ev
				     where ev.OwnerObjectID = @ObjectID
				     and ev.ObjectTypeSysName = @ObjectTypeSysName2
				    )
		begin

	  		exec dbo.extvalueadd
	   		  @ID = null
	  		, @ObjectTypeSysName = @ObjectTypeSysName2
	  		, @OwnerObjectID = @ObjectID

		end

			exec dbo.EventAdd
			  @ID = null
			, @ObjectTypeSysName = 'EventUpd'
			, @OwnerObjectID = @ObjectID
			, @OwnerObjectTypeID = null
			, @Message = 'Создание склада по заявке'          
			, @Descript = @SDnumber
			
			insert into #res (Message, id)
			values ('Импортирован склад и добавлены характеристики', @ObjectID)

	 commit tran
	end try  

	begin catch

		  insert into #res (Message, id)
		  values (ERROR_MESSAGE(), @ObjectID)
	
	end catch
end

select *
from #res r 