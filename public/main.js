document.querySelectorAll('#trend-button').forEach(trendButton =>
    trendButton.addEventListener('click', _ =>{
        fetch('/names',{
            method: 'put',
            headers: {'Content-Type': 'application/json'},
            body: JSON.stringify({button: trendButton.value})
        }).then(data => {
            window.location.reload()
        })
    })
)

document.querySelectorAll('#delete-button').forEach(deleteButton =>

    deleteButton.addEventListener('click', _ => {
        fetch('/names',{
            method: 'delete',
            headers: { 'Content-Type': 'application/json'},
            body: JSON.stringify({
                _id: deleteButton.value
            })
        }).then(res=> {
            if(res.ok) return res.json()
        }).then(data => {
            window.location.reload()
    })
}))

document.querySelectorAll('td.match-cell').forEach(matchCell =>
    matchCell.addEventListener('click', _ => {
        fetch('/ner/thread',{
            method: 'put',
            headers: { 'Content-Type': 'application/json'},
            body: JSON.stringify({
                _id: matchCell.getAttribute('data-id')
            })
        }).then(res => {
            if(res.ok) return res.json()
        }).then(data => {
            window.location.assign('/ner/thread')
        })
}))