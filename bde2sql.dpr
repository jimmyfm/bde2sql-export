program bde2sql;

{$APPTYPE CONSOLE}

{$R *.res}

uses
  System.SysUtils, Bde.DBTables, System.Classes, Data.DB;

const
  DB_NAME : String = '';
var
  tblFrom: TTable;
  dbFrom: TDatabase;

  app: TStringList;
  i, c, d: integer;
  fd, fieldDef, newFieldDef: TFieldDef;
  str, prefix: string;
  logFile: TextFile;
  idx: TIndexDef;

begin
  try
    dbFrom := TDatabase.Create(nil);
    dbFrom.AliasName := DB_NAME;
    dbFrom.DatabaseName := DB_NAME;
    dbFrom.Name := DB_NAME;

    tblFrom := TTable.Create(nil);
    tblFrom.DatabaseName := DB_NAME;

    app := TStringList.Create;

    dbFrom.Open;

    dbFrom.GetTableNames(app, false);
    for i := 0 to app.Count - 1 do
    begin
      AssignFile(logFile, app[i] + '.sql');
      Rewrite(logFile);
      Write(logFile, 'DROP TABLE IF EXISTS dbo.' + app.Strings[i] + ';' + #10);

      tblFrom.TableName := app[i];
      tblFrom.Open;

      Write(logFile, 'CREATE TABLE ' + app[i] + ' (');
      for c := 0 to tblFrom.FieldDefs.Count - 1 do
      begin
        fd := tblFrom.FieldDefs[c];
        str := ' ';
        case fd.DataType of
          ftAutoInc:
            str := 'int IDENTITY (1,1) NOT NULL, CONSTRAINT PK_' + app[i] + '_' + fd.Name + ' PRIMARY KEY CLUSTERED (' +
              fd.Name + ')';
          ftInteger:
            str := 'int';
          ftString:
            str := 'varchar(' + IntToStr(fd.Size) + ') COLLATE Latin1_General_CI_AS';
          ftFloat:
            str := 'float';
          ftDate:
            str := 'date';
          ftBoolean:
            str := 'bit';
          ftSmallint:
            str := 'smallint';
          ftMemo:
            str := 'text';
          ftDateTime:
            str := 'datetime';
          ftTime:
            str := 'time';
          ftBlob:
            str := 'nvarchar(max)';
        else
          Write(logFile, #10 + 'Campo STRANO: ' + FieldTypeNames[fd.DataType]);
        end;
        if fd.Required then
          str := str + ' NOT NULL';
        str := ' ' + fd.Name + ' ' + str;

        if c < tblFrom.FieldDefs.Count - 1 then
          str := str + ',';

        Write(logFile, str);
      end;
      Write(logFile, ');' + #10);

      if not tblFrom.IsEmpty then
      begin
        Write(logFile, 'SET IDENTITY_INSERT dbo.' + app[i] + ' ON;' + #10);
        prefix := 'INSERT INTO dbo.' + app[i] + ' (';
        for c := 0 to tblFrom.FieldDefs.Count - 1 do
        begin
          prefix := prefix + tblFrom.FieldDefs[c].Name;

          if c < tblFrom.FieldDefs.Count - 1 then
            prefix := prefix + ',';
        end;
        prefix := prefix + ') VALUES ';
        Write(logFile, '-- Records: ' + IntToStr(tblFrom.RecordCount) + #10);

        tblFrom.First;
        d := 0;
        str := '';
        while not tblFrom.Eof do
        begin
          Inc(d);
          str := str + '(';
          for c := 0 to tblFrom.Fields.Count - 1 do
          begin
            if tblFrom.Fields[c].IsNull then
              str := str + 'null'
            else
              case tblFrom.Fields[c].DataType of
                ftString, ftBoolean, ftSmallint, ftMemo:
                  str := str + #39 + StringReplace(tblFrom.Fields[c].AsString, #39, #39#39, [rfReplaceAll]) + #39;
                ftAutoInc, ftInteger, ftFloat:
                  str := str + ' ' + tblFrom.Fields[c].AsString;
                ftDate:
                  str := str + #39 + FormatDateTime('yyyy-mm-dd', tblFrom.Fields[c].AsDateTime) + #39;
                ftDateTime:
                  str := str + #39 + FormatDateTime('yyyy-mm-dd hh:mm:ss', tblFrom.Fields[c].AsDateTime) + #39;
                ftTime:
                  str := str + #39 + FormatDateTime('hh:mm:ss', tblFrom.Fields[c].AsDateTime) + #39;
              else
                // ShowMessage('issues with column: ' + tblFrom.Fields[c].Name);
              end;

            if c < tblFrom.FieldDefs.Count - 1 then
              str := str + ',';
          end;

          if d mod 500 = 0 then
          begin
            Write(logFile, prefix + str + ');' + #10);
            str := '';
          end;

          tblFrom.Next;

          if (d mod 500 <> 0) and (not tblFrom.Eof) then
            str := str + '),';
        end;

        if d mod 500 <> 0 then
          Write(logFile, prefix + str + ');' + #10);

        // Write(logFile, ';');
        Write(logFile, 'SET IDENTITY_INSERT dbo.' + app[i] + ' OFF;' + #10);
      end;

      for c := 0 to tblFrom.IndexDefs.Count - 1 do
      begin
        idx := tblFrom.IndexDefs[c];
        if ixPrimary in idx.Options then
          Continue;

        str := 'CREATE ';
        if ixUnique in idx.Options then
          str := str + 'UNIQUE ';

        Write(logFile, str + 'INDEX "' + idx.DisplayName + '" ON dbo.' + app[i] + ' (' + StringReplace(idx.Fields, ';',
          ',', [rfReplaceAll]) + ');' + #10);
        if ixDescending in idx.Options then
          Write(logFile, '-- ixDescending' + #10);
        // XXX Managed via CI collation
        // if ixCaseInsensitive in idx.Options then
        // Write(logFile, '-- ixCaseInsensitive' + #10);
        if ixExpression in idx.Options then
          Write(logFile, '-- ixExpression' + #10);
        if ixNonMaintained in idx.Options then
          Write(logFile, '-- ixNonMaintained' + #10);
      end;

      tblFrom.Close;
      CloseFile(logFile);
    end;

  except
    on E: Exception do
      Writeln(E.ClassName, ': ', E.Message);
  end;
end.
