use lozon;
go

declare
    @sysname				   dbo.string    = 'StateSchemeCarriageToDecisionRequired'  -- sysname действия
  , @srcstateid				   dbo.bident  
  , @dststateid                dbo.bident  
  , @action                    dbo.string
  , @procname                  dbo.string
  , @statetransitid            dbo.bident  

declare @res table 
    ( action                   dbo.string
    , proctype                 dbo.string
    , [sysname]                dbo.string
    , sp_helptext              dbo.string    )

select
    @statetransitid      = st.id
    , @srcstateid		 = st.srcstateid
    , @dststateid		 = st.dststateid
    , @action			 = o.name
from
    dbo.statetransit st 
    inner join dbo.[object] o on (o.id = st.id)
where 
    o.[sysname] = @sysname

declare [before_transit] cursor local static forward_only read_only for  
    select  
        sttlm.[sysname]  
    from  
        dbo.statetransittrigger sttlm --with (noexpand)  
    where  
        (sttlm.ownerobjectid = @statetransitid)  
        and (sttlm.objecttypeid = 1541000) -- extvalueprocbeforetransit  
    order by  
        sttlm.ord asc;  
  
open [before_transit];  
  
    fetch [before_transit]  
    into  
        @procname;  
  
    while (@@fetch_status = 0)  
    begin  
        if (nullif(trim(@procname), '') is null)  
        begin  
            execute dbo.[abort]  
                  @code = @@procid  
                , @message = 'Ссылка на процедуру "Перед переходом состояний" указывает на процедуру, отсутствующую на сервере. "%s"'  
                , @p1 = @procname;  
        end;  
  
    insert into @res ([action], proctype, [sysname], sp_helptext)
    values
        (
              @action
            , 'extvalueprocbeforetransit'
            , @procname
            , concat('sp_helptext ', quotename(@procname, ''''))
        );

    fetch [before_transit]  
        into  
            @procname;  
    end;  
  
close [before_transit];

deallocate [before_transit];  

declare [on_exit_from_state] cursor local static forward_only read_only for  
    select  
        sttlm.[sysname]  
    from  
        dbo.statetransittrigger sttlm --with (noexpand)  
    where  
        (sttlm.ownerobjectid = @srcstateid)  
        and (sttlm.objecttypeid = 1547000) -- extvalueproconexitformstate  
    order by  
        sttlm.ord asc;  
  
open [on_exit_from_state];  
  
    fetch [on_exit_from_state]  
    into  
        @procname;  
    while (@@fetch_status = 0)  
    begin  
        if (nullif(trim(@procname), '') is null)  
        begin  
            execute dbo.[abort]  
              @code = @@procid  
            , @message = 'Ссылка на процедуру "При выходе из состояния" указывает на процедуру, отсутствующую на сервере. "%s"'  
            , @p1 = @procname;  
    end;  
  
    insert into @res ([action], proctype, [sysname], sp_helptext)
    values
        (
              @action
            , 'extvalueproconexitformstate'
            , @procname
            , concat('sp_helptext ', quotename(@procname, ''''))
        );
  
    fetch [on_exit_from_state]  
    into  
        @procname;  
    end;  
  
close [on_exit_from_state];

deallocate [on_exit_from_state];  

    insert into @res ([action], proctype)
    values
        (
              @action
              , 'update object'
        ); 

declare [on_enter_in_state] cursor local static forward_only read_only for  
    select  
        sttlm.[sysname]  
    from  
        dbo.statetransittrigger sttlm --with (noexpand)  
    where  
        (sttlm.ownerobjectid = @dststateid)  
        and (sttlm.objecttypeid = 1545000) -- extvalueproconenterinstate  
    order by  
        sttlm.ord asc;  
  
open [on_enter_in_state];  
  
    fetch [on_enter_in_state]  
    into  
        @procname;  
  
    while (@@fetch_status = 0)  
    begin  
        if (nullif(trim(@procname), '') is null)  
        begin  
            execute dbo.[abort]  
              @code = @@procid  
            , @message = 'Ссылка на процедуру "При входе в состояние" указывает на процедуру, отсутствующую на сервере. "%s"'  
            , @p1 = @procname;  
        end;  
  
    insert into @res ([action], proctype, [sysname], sp_helptext)
    values
        (
              @action
            , 'extvalueproconenterinstate'
            , @procname
            , concat('sp_helptext ', quotename(@procname, ''''))
        );
  
    fetch [on_enter_in_state]  
    into  
        @procname;  
    end;  
  
close [on_enter_in_state];

deallocate [on_enter_in_state];  

declare [after_transit] cursor local static forward_only read_only for  
    select  
        sttlm.[sysname]  
    from  
        dbo.statetransittrigger sttlm --with (noexpand)  
    where  
        (sttlm.ownerobjectid = @statetransitid)  
        and (sttlm.objecttypeid = 1543000) -- extvalueprocaftertransit  
    order by  
        sttlm.ord asc;
  
open [after_transit];  
  
    fetch [after_transit]  
    into  
        @procname;  
  
    while (@@fetch_status = 0)  
    begin  
    if (nullif(trim(@procname), '') is null)  
        begin  
            execute dbo.[abort]  
              @code = @@procid  
            , @message = 'Ссылка на процедуру "После перехода состояний" указывает на процедуру, отсутствующую на сервере. "%s"'  
            , @p1 = @procname;  
    end;  
  
    insert into @res ([action], proctype, [sysname], sp_helptext)
    values
        (
              @action
            , 'extvalueprocaftertransit'
            , @procname
            , concat('sp_helptext ', quotename(@procname, ''''))
        ); 
  
    fetch [after_transit]  
    into  
        @procname;  
    end;  
  
close [after_transit];  
deallocate [after_transit];  

select * from @res