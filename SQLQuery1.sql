--1.	Создать хранимую процедуру, которая:
--a.	добавляет каждой книге два случайных жанра;
--b.	отменяет совершённые действия, если в процессе работы хотя бы одна операция вставки завершилась ошибкой в силу дублирования значения первичного ключа таблицы «m2m_books_genres» (т.е. у такой книги уже был такой жанр).

select books.b_id, b_name, STRING_AGG(g_name, ',')
from books
inner join m2m_books_genres on books.b_id = m2m_books_genres.b_id
inner join genres on genres.g_id = m2m_books_genres.g_id
group by books.b_id, books.b_name;
go

create procedure TWO_RANDOM_GENRES
as
begin
	declare @b_id_value int;
	declare @g_id_value int;
	declare genres_cursor cursor local fast_forward for
		select top 2 [g_id]
		from dbo.genres
		order by NEWID();
	declare book_cursor cursor local fast_forward for
		select b_id
		from dbo.books;
	declare @fetch_genres_cursor int;
	declare @fetch_book_cursor int;
	print 'starting transaction...';
	begin transaction;

	open book_cursor;
	fetch next from book_cursor into @b_id_value;
	set @fetch_book_cursor = @@FETCH_STATUS;

	while @fetch_book_cursor = 0
	begin
		open  genres_cursor
		fetch next from genres_cursor into @g_id_value
		set @fetch_genres_cursor = @@FETCH_STATUS
		while @fetch_genres_cursor = 0
		begin
			begin TRY
				insert into m2m_books_genres
					(b_id,
					 g_id)
				values (@b_id_value,
						@g_id_value);
			end TRY
			begin CATCH
				print 'Error! Transaction not completed...';
				rollback transaction;
				return;
			end CATCH
			fetch next from genres_cursor into @g_id_value;
			set @fetch_genres_cursor = @@FETCH_STATUS
		end;
		close genres_cursor;
		fetch next from book_cursor into @b_id_value;
		set @fetch_book_cursor = @@FETCH_STATUS;
	end;
	close book_cursor;
	deallocate book_cursor;
	deallocate genres_cursor;
	commit transaction
end;
go

EXECUTE TWO_RANDOM_GENRES;
drop procedure TWO_RANDOM_GENRES;
go

--2.	Создать хранимую процедуру, которая:
--a.	увеличивает значение поля «b_quantity» для всех книг в два раза;
--b.	отменяет совершённое действие, если по итогу выполнения операции среднее количество экземпляров книг превысит значение 50.

select books.b_id, b_name, b_quantity, b_year
from books
go

select SUM(b_quantity)
from books
go

select COUNT(*)
from books
go

create procedure MULTIPLE_QUANTITY_UP_TWICE
as
begin
	declare @b_id_value int;
	declare @b_name_value nvarchar(150);
	declare @b_quantity_value smallint;
	declare @b_year_value smallint;
	declare @quantity_count smallint;
	declare @books_count smallint;
	set @quantity_count = 0;
	declare book_cursor cursor local fast_forward for
		select b_id, b_name, b_quantity, b_year
		from dbo.books;
	declare @fetch_book_cursor int;
	print 'starting transaction...';
	begin transaction;
	open book_cursor;
	fetch next from book_cursor into @b_id_value, @b_name_value, @b_quantity_value, @b_year_value; 
	set @fetch_book_cursor = @@FETCH_STATUS;

	while @fetch_book_cursor = 0
	begin
		update books set
				 b_quantity = @b_quantity_value * 2
		where b_id = @b_id_value

		set @quantity_count += @b_quantity_value*2;
		fetch next from book_cursor into @b_id_value, @b_name_value, @b_quantity_value, @b_year_value;
		set @fetch_book_cursor = @@FETCH_STATUS;
	end;
	close book_cursor;
	deallocate book_cursor;
	print CAST(@quantity_count as varchar);
	select @books_count = COUNT(*) from books;
	set @quantity_count = @quantity_count / @books_count
	print CAST(@quantity_count as varchar);
	if @quantity_count > 50 
		begin
			print 'rolling transaction back';
			rollback transaction;
		end;
	else
		begin
			print 'commit transaction';
			commit transaction;
		end;
end;
go

EXECUTE MULTIPLE_QUANTITY_UP_TWICE;
drop procedure MULTIPLE_QUANTITY_UP_TWICE;
go

--3.	Написать запросы, которые, будучи выполненными параллельно, обеспечивали бы следующий эффект:
--a.	первый запрос должен считать количество выданных на руки и возвращённых в библиотеку книг и не зависеть от запросов на обновление таблицы «subscriptions» (не ждать их завершения);
--b.	второй запрос должен инвертировать значения поля «sb_is_active» таблицы subscriptions с «Y» на «N» и наоборот и не зависеть от первого запроса (не ждать его завершения).

select @@SPID as session_id
set implicit_transactions on
set transaction isolation level read uncommitted;
begin transaction

select COUNT(sb_id) as inactive
from subscriptions
where sb_is_active = 'N'

select COUNT(sb_id) as active
from subscriptions
where sb_is_active = 'Y'
commit transaction


select @@SPID as session_id
set implicit_transactions on 
begin transaction 
update subscriptions
set [sb_is_active] =
case
	when [sb_is_active] = 'Y' THEN 'N'
	when [sb_is_active] = 'N' THEN 'Y'
end;--waitfor delay '00:00:10';
commit transaction
go
--5.	Написать код, в котором запрос, инвертирующий значения поля «sb_is_active» таблицы 
--«subscriptions» с «Y» на «N» и наоборот, будет иметь максимальные шансы на успешное 
--завершение в случае возникновения ситуации взаимной блокировки с другими транзакциями.
set DEADLOCK_PRIORITY 10;
update subscriptions
set [sb_is_active] =
case
	when [sb_is_active] = 'Y' THEN 'N'
	when [sb_is_active] = 'N' THEN 'Y'
end;
set DEADLOCK_PRIORITY normal;
go
--6.	Создать на таблице «subscriptions» триггер, определяющий уровень изолированности 
--транзакции, в котором сейчас проходит операция обновления, и отменяющий операцию, 
--если уровень изолированности транзакции отличен от REPEATABLE READ.
create trigger [subscriptions_upd_trans]
on subscriptions
after insert
as 
	DECLARE @isolation_level NVARCHAR(50);
	SET @isolation_level =
	(
	 SELECT [transaction_isolation_level]
	 FROM [sys].[dm_exec_sessions]
	 WHERE [session_id] = @@SPID
	);
	IF (@isolation_level != 3)
	 BEGIN
	 RAISERROR ('Please, switch your transaction to REPEATABLE READ isolation
	 level and rerun this INSERT again.', 16, 1);
	 ROLLBACK TRANSACTION;
	 RETURN
 END;
 go

 drop trigger [subscriptions_upd_trans];
 go

SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;
INSERT INTO [subscriptions]
 ([sb_book],
 [sb_start],
 [sb_finish],
 [sb_is_active],
 [sb_subscriber])
VALUES (1,
 GETDATE(),
 GETDATE(),
 N'Y',
 1);