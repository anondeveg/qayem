import fitz

doc = fitz.open()
page = doc.new_page(width=595, height=842) # A4 size

# Highlight 1
text_rect = fitz.Rect(50, 100, 500, 130)
annot = page.add_highlight_annot(text_rect)
annot.set_colors(stroke=(1, 1, 0)) # yellow
annot.update()
page.insert_textbox(text_rect, "This is a test highlight quote", fontsize=16)

# Highlight 2
text_rect2 = fitz.Rect(50, 200, 500, 230)
annot2 = page.add_highlight_annot(text_rect2)
annot2.set_colors(stroke=(1, 1, 0)) # yellow
annot2.update()
page.insert_textbox(text_rect2, "Welcome to Qayem application", fontsize=16)

doc.save("test_highlight.pdf")
doc.close()
print("Created test_highlight.pdf successfully!")
