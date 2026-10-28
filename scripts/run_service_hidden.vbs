' Script untuk menjalankan server FastAPI di background Windows tanpa jendela terminal.
' Tetap berjalan meskipun browser atau command prompt ditutup.
Set WshShell = CreateObject("WScript.Shell")
strPath = WshShell.CurrentDirectory
WshShell.Run "cmd /c """ & strPath & "\scripts\run_server.bat""", 0, False
Set WshShell = Nothing
