rowCount = 1;
preDataLength = 0;
recName = "Unknown";

if (!!window.EventSource) {
    if (typeof (EventSource) !== "undefined") {
        console.log("Gait Similarity Results");

        var source1 = new EventSource('/get_similarity');

        source1.onmessage = function (e) {
            $("tbody").empty();
            data = JSON.parse(e.data);
            // console.log(data);
            // $("#gaitResult").text(JSON.stringify(data));

            keys = Object.keys(data);
            values = Object.values(data);


           // Find the first valid name (ignoring "None")
            for (let i = 0; i < keys.length; i++) {
                let name = keys[i].split("-")[0];
                let similarity = values[i]['similarity'] * 100;
                
                if (name.toLowerCase() !== "none" && !/^\d+$/.test(name) && similarity > 50) {
   		recName = name;
   		break;

                }
            }

            document.getElementById('gaitResult').innerHTML = `Recognized person is <strong class="text-danger">${recName}.</strong>`;
            document.getElementById('spinner1').style.display = 'none';

            curDataLength = keys.length;
            lengthDiff = curDataLength - preDataLength;
            Table = document.getElementById('table');
            document.getElementById('spinner2').style.display = 'none';
            for (i = 0; i < lengthDiff; i++) {
                row = Table.insertRow(-1);
                n = curDataLength - lengthDiff + i
                label = keys[n].split("-")
				if (label[0].toLowerCase() !== "none" && !/^\d+$/.test(label[0])) {
				row.insertCell(0).innerHTML = rowCount;
				row.insertCell(1).innerHTML = label[0];
				row.insertCell(2).innerHTML = label[1];
				row.insertCell(3).innerHTML = (values[n]['dist']).toFixed(3);
				row.insertCell(4).innerHTML = (values[n]['similarity'] * 100).toFixed(3);
				rowCount++;
			}

                preDataLength = curDataLength;
            }
            source1.close()
        }

    } else {
        console.log("No server sent support")
    }
};