import React, {useState} from 'react';
import Button from 'react-bootstrap/Button'

function Upload(){
	const [selectedFile, setSelectedFile] = useState();
	const [isSelected, setIsFilePicked] = useState(false);

	const changeHandler = (event) => {
		setSelectedFile(event.target.files[0]);
		setIsFilePicked(true);
	};

	const handleSubmission = () => {
	};

	return(
   <div>
			<input type="file" name="file" onChange={changeHandler} />
			
			<div>
				<Button onClick={handleSubmission}>Submit</Button>
			</div>
		</div>
	)
}
export default Upload;