--1.	������� �������� ���������, �������:
--a.	��������� ������ ����� ��� ��������� �����;
--b.	�������� ����������� ��������, ���� � �������� ������ ���� �� ���� �������� ������� ����������� ������� � ���� ������������ �������� ���������� ����� ������� �m2m_books_genres� (�.�. � ����� ����� ��� ��� ����� ����).
create procedure TWO_RANDOM_GENRES
as
begin
	declare @b_id_value int;
	declare @g_id_value int;
	declare genres_cursor cursor local fast_forward for
		select g_id
		from dbo.genres;
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

	end;
end;
go;