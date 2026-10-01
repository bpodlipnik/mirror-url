-- Keep cross-guide navigation in the rendered HTML guides.
function Link(link)
    link.target = link.target:gsub("^%./(USER_GUIDE)%.md", "./%1.html")
    link.target = link.target:gsub("^%./(DEVELOPER_GUIDE)%.md", "./%1.html")
    return link
end
